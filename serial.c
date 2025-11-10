#define _GNU_SOURCE
#include "serial.h"

#include <pthread.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <limits.h>
#include <assert.h>
#include <zlib.h>

#define BUFFER_SIZE 1048576 

/* --------------------------------------------------------------------------
   here is an outline of the plan
   1. we scan input_dir for .txt files and sort names in lex order
   2. we build a results table with one slot per file
   3. we push one job per file into a shared queue
   4. workers read and compress in parallel
   5. we write members in lex order using the starter writer
   note we keep final writes single threaded to avoid container issues
   -------------------------------------------------------------------------- */

/* ----------------------------- data models ------------------------------ */

typedef struct job {
    char *path;           // full path to disk file
    int index;            // position in lex order
    struct job *next;     // next job in queue
} job_t;                    // job structure

typedef struct {
    unsigned char **out_bufs;  // one buffer per file
    int *out_sizes;            // compressed size per file
    int *in_sizes;             // uncompressed size per file
    int count;                 // number of files
} results_t;                 // results table

/* ---------------------------- work queue core --------------------------- */

typedef struct {
    pthread_mutex_t mtx;        // mutex for queue operations
    pthread_cond_t cv_have;     // condition var for job availability
    pthread_cond_t cv_done;     // condition var for queue completion
    job_t *head;                // head of the job queue
    job_t *tail;                // tail of the job queue
    int open;                   // queue is open while producer pushes
    int active;                 // workers currently holding a job
    results_t *res;             // pointer to results table
} queue_t;                       // queue structure

// here we initialize the queue with locks and condition variables
static void queue_init(queue_t *q, results_t *res) {
    pthread_mutex_init(&q->mtx, NULL);        // init mutex for thread safety
    pthread_cond_init(&q->cv_have, NULL);     // init cv for signaling job availability
    pthread_cond_init(&q->cv_done, NULL);     // init cv for signaling completion
    q->head = q->tail = NULL;                 // empty queue initially
    q->open = 1;                              // queue is open for new jobs
    q->active = 0;                            // no active workers yet
    q->res = res;                             // link to results storage
}

// here we close the queue to signal no more jobs will be added
static void queue_close(queue_t *q) {
    pthread_mutex_lock(&q->mtx);          
    q->open = 0;                          
    pthread_cond_broadcast(&q->cv_have);    
    pthread_mutex_unlock(&q->mtx);         
}

// here we add a job to the end of the queue
static void queue_push(queue_t *q, job_t *j) {
    j->next = NULL;                 
    pthread_mutex_lock(&q->mtx);              // acquire lock
    if (q->tail) q->tail->next = j;           // append to existing tail
    else q->head = j;                         // or set as first job
    q->tail = j;                           
    pthread_cond_signal(&q->cv_have);         // signal that a job is available
    pthread_mutex_unlock(&q->mtx);         
}

// here we remove and return a job from the queue, blocking if empty
static job_t* queue_pop(queue_t *q) {
    pthread_mutex_lock(&q->mtx);              // acquire lock
    while (q->head == NULL && q->open) {      // wait while queue is empty and open
        pthread_cond_wait(&q->cv_have, &q->mtx);  // sleep until signaled
    }
    job_t *job = q->head;                     // get job from head
    if (job) {
        q->head = job->next;                
        if (!q->head) q->tail = NULL;         // clear tail if queue now empty
        job->next = NULL;                     // detach job from queue
        q->active++;                     
    }
    pthread_mutex_unlock(&q->mtx);            // release lock
    return job;                               // return job or NULL if queue closed
}

// here we mark that a worker has completed a task
static void queue_task_done(queue_t *q) {
    pthread_mutex_lock(&q->mtx);              // acquire lock
    q->active--;                              // decrement active worker count
    if (!q->open && !q->head && q->active == 0) {  // check if all work is done
        pthread_cond_signal(&q->cv_done);     // signal completion
    }
    pthread_mutex_unlock(&q->mtx);            // release lock
}

// here we wait for all jobs to be completed
static void queue_wait_all(queue_t *q) {
    pthread_mutex_lock(&q->mtx);              // acquire lock
    while (q->open || q->head || q->active) { // wait while work remains
        pthread_cond_wait(&q->cv_done, &q->mtx);  // sleep until signaled
    }
    pthread_mutex_unlock(&q->mtx);            // release lock
}

/* --------------------------- worker threads ---------------------------- */

typedef struct { queue_t *q; } worker_arg_t;

// here we free the memory allocated for a job
static void free_job(job_t *j) {
    if (!j) return;
    free(j->path);  
    free(j);   
}

// here we define the worker thread function that processes compression jobs
static void* worker_main(void *arg) {
    worker_arg_t *wa = (worker_arg_t*)arg;
    queue_t *q = wa->q;
    
    for (;;) {
        job_t *j = queue_pop(q);              // get next job from queue
        if (!j) break;                        // NULL means queue is closed and empty

        unsigned char buffer_in[BUFFER_SIZE];
        unsigned char buffer_out[BUFFER_SIZE];

        // load file
        FILE *f_in = fopen(j->path, "r");
        if (f_in) {
            int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
            fclose(f_in);

            // Store input size
            results_t *res = q->res;
            if (res && j->index >= 0 && j->index < res->count) {
                res->in_sizes[j->index] = nbytes;  // track uncompressed size
            }

            // zip file
            z_stream strm;
            int ret = deflateInit(&strm, 9);  // init zlib with compression level 9
            if (ret == Z_OK) {
                strm.avail_in = nbytes;      
                strm.next_in = buffer_in;   
                strm.avail_out = BUFFER_SIZE; 
                strm.next_out = buffer_out;   

                ret = deflate(&strm, Z_FINISH);  // compress all at once
                if (ret == Z_STREAM_END) {
                    // dump zipped file
                    int nbytes_zipped = BUFFER_SIZE - strm.avail_out;  // calculate compressed size
                    
                    results_t *res = q->res;
                    if (res && j->index >= 0 && j->index < res->count) {
                        res->out_bufs[j->index] = malloc(nbytes_zipped);
                        if (res->out_bufs[j->index]) {
                            memcpy(res->out_bufs[j->index], buffer_out, nbytes_zipped);  // copy compressed data
                            res->out_sizes[j->index] = nbytes_zipped;  // store compressed size
                        }
                    }
                }
                deflateEnd(&strm);  // cleanup zlib state
            }
        }
        
        queue_task_done(q);  
        free_job(j);         
    }
    return NULL;
}

/* ----------------------------- public entry ----------------------------- */

// here we compare two strings for qsort lexicographical ordering
static int cmp(const void *a, const void *b) {
    return strcmp(*(char **)a, *(char **)b);
}

// here we compress all text files in a directory using parallel workers
void compress_directory(char *directory_name) {
    DIR *d;
    struct dirent *dir;
    char **files = NULL;
    int nfiles = 0;

    d = opendir(directory_name);  
    if (d == NULL) {
        printf("An error has occurred\n");
        return;
    }

    // create sorted list of text files
    while ((dir = readdir(d)) != NULL) {
        int len = strlen(dir->d_name);
        if (len >= 4 && dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && 
            dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {  // check for .txt extension
            files = realloc(files, (nfiles + 1) * sizeof(char *));  // grow array
            assert(files != NULL);
            files[nfiles] = strdup(dir->d_name);  // copy filename
            assert(files[nfiles] != NULL);
            nfiles++;
        }
    }
    closedir(d);
    qsort(files, nfiles, sizeof(char *), cmp);  // sort filenames lexicographically

    results_t res = {0};
    res.count = nfiles;
    res.out_bufs = calloc(nfiles, sizeof(unsigned char*));  // allocate result buffers
    res.out_sizes = calloc(nfiles, sizeof(int));  // allocate size arrays
    res.in_sizes = calloc(nfiles, sizeof(int));
    assert(res.out_bufs != NULL && res.out_sizes != NULL && res.in_sizes != NULL);

    queue_t q;
    queue_init(&q, &res);                     // initialize work queue

    int nthreads = 4;                         // use 4 worker threads
    if (nthreads > 19) nthreads = 19;         // enforce max thread limit
    pthread_t ths[nthreads];
    worker_arg_t wa = {.q = &q};
    
    for (int i = 0; i < nthreads; i++) {
        pthread_create(&ths[i], NULL, worker_main, &wa);  // spawn worker threads
    }

    for (int i = 0; i < nfiles; i++) {
        job_t *j = calloc(1, sizeof(job_t));  // create new job
        assert(j != NULL);
        
        int len = strlen(directory_name) + strlen(files[i]) + 2;
        j->path = malloc(len * sizeof(char));  // build full path
        assert(j->path != NULL);
        strcpy(j->path, directory_name);
        strcat(j->path, "/");
        strcat(j->path, files[i]);
        j->index = i;                         // set position in result array
        
        queue_push(&q, j);                    // add job to queue
    }

    queue_close(&q);                          // signal no more jobs coming
    queue_wait_all(&q);                       // wait for all jobs to complete

    for (int i = 0; i < nthreads; i++) {
        pthread_join(ths[i], NULL);           // wait for worker threads to exit
    }

    // create a single zipped package with all text files in lexicographical order
    int total_in = 0, total_out = 0;
    FILE *f_out = fopen("text.tzip", "w");    // open output file
    assert(f_out != NULL);
    
    for (int i = 0; i < nfiles; i++) {
        if (res.out_bufs[i] && res.out_sizes[i] > 0) {
            fwrite(&res.out_sizes[i], sizeof(int), 1, f_out);  // write compressed size
            fwrite(res.out_bufs[i], sizeof(unsigned char), res.out_sizes[i], f_out);  // write compressed data
            total_in += res.in_sizes[i];   
            total_out += res.out_sizes[i];   
        }
    }
    fclose(f_out);

    printf("Compression rate: %.2lf%%\n", 100.0 * (total_in - total_out) / (double)total_in);

    // release list of files
    for (int i = 0; i < nfiles; i++) {
        free(files[i]);     
        free(res.out_bufs[i]);
    }
    free(files);
    free(res.out_bufs);
    free(res.out_sizes);
    free(res.in_sizes);
}