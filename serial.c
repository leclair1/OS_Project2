///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Course: COP 4600 
// Group: 4
// Members:
//     Paige Leclair -  leclair1@usf.edu
//     Laura Robayo  – laurarobayo@usf.edu
//     Nusraat Kabir - nkabir@usf.edu
//
///////////////////////////////////////////// - Description - ////////////////////////////////////////////////////////////////////////
// For project 2, we parallelized the baseline compressor using pthreads to speed up text file compression.
// Program scans the directory for .txt files, sorts them lexicographically, and pushes one job per file into a synchronized queue. 
// Worker threads read and deflate files concurrently, storing results in indexed buffers. 
// The main thread writes all compressed data sequentially into text.tzip to preserve order.
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#define _GNU_SOURCE // to allow GNU extensions
#include "serial.h" // for our header file

#include <pthread.h>    // for thread creation and synchronization
#include <dirent.h>      // reading directory contents
#include <stdio.h>       //  standard input/output functions
#include <stdlib.h>      //  memory allocation and general utilities
#include <string.h>  // for string handling functions
#include <stdint.h>  //  fixed-size integer types like uint32_t

// for this, we only include <unistd.h> if we're not on Windows (so POSIX-only functions)
#if !defined(_WIN32)
#include <unistd.h>
#endif

#include <zlib.h> // for compression and decompression (zlib library)

// thread configuration constants
#define BUFFER_SIZE 1048576 // 1MB buffer size
#ifndef MAX_WORKER_THREADS // ensures we always have a max threads limit
#define MAX_WORKER_THREADS 19 // cap total threads to 19 (increased for better performance)
#endif

/* -------------------------------overview-------------------------------------------
   1) list *.txt files in lexicographic order
   2) push one job per file into a shared queue
   3) workers read + compress each file
   4) main writes [uint32 size][bytes] for every file into text.tzip
   -------------------------------------------------------------------------- */

/* ----------------------------- data models ------------------------------ */
// job_t represents one file waiting to be compressed.
typedef struct job {
    char *path;            // full path to disk file
    int   index;           // position in lexicographical order
    struct job *next; // pointer to the next job in the queue
} job_t;

// results_t holds the data produced by all worker threads.
typedef struct {
    unsigned char **out_bufs;  // compressed data buffers per file 
    int            *out_sizes; // compressed sizes (bytes) 
    int            *in_sizes;  // original sizes (bytes) 
    int             count;     //total number of files 
} results_t;

/* ---------------------------- work queue core --------------------------- */

typedef struct {
    pthread_mutex_t mtx;      // protects everything below 
    pthread_cond_t cv_have;   // signaled when a new job arrives 
    pthread_cond_t cv_done;   // signaled when work is fully finished 
    job_t *head;              //first job in the list 
    job_t *tail;              // last job in the list 
    int open;              // 1 while producer may still push jobs 
    int active;            // number of workers currently processing 
    results_t *res;        // pointer to results table
} queue_t;

static void queue_init(queue_t *q, results_t *res) { // initializing the queue
    pthread_mutex_init(&q->mtx, NULL);        // lock protects shared data
    pthread_cond_init(&q->cv_have, NULL);     // signals when a job is added
    pthread_cond_init(&q->cv_done, NULL);     // signals when all work is done
    q->head = NULL;           // no first job yet
    q->tail = NULL;         // no last job yet
    q->open = 1;            // still accepting new jobs
    q->active = 0;            // no workers currently busy
    q->res = res;             // store pointer to results
}

static void queue_close(queue_t *q) { // closing the queue
    pthread_mutex_lock(&q->mtx); // locking the mutex
    q->open = 0; // setting the open flag to 0
    pthread_cond_broadcast(&q->cv_have); // broadcast the condition variable
    pthread_mutex_unlock(&q->mtx); // unlocking the mutex
}

static void queue_push(queue_t *q, job_t *j) { // pushing a job to the queue
    j->next = NULL; // setting the next pointer to NULL
    pthread_mutex_lock(&q->mtx); // locking the mutex
    if (q->tail) // if the tail is not NULL
        q->tail->next = j; // then we set the next pointer of the tail to the job
    else // if the tail IS NULL
        q->head = j; // then we set the head to the job
    q->tail = j; // setting the tail to the job
    pthread_cond_signal(&q->cv_have); // signalling the condition variable (use signal not broadcast)
    pthread_mutex_unlock(&q->mtx); // unlocking the mutex
}

static job_t* queue_pop(queue_t *q) { // popping a job from the queue
    pthread_mutex_lock(&q->mtx); // lock the mutex
    while (!q->head && q->open) { // while the head is NULL and the queue is open
        pthread_cond_wait(&q->cv_have, &q->mtx); // we wait for the condition variable
    }
    job_t *j = q->head; // getting the head of the queue
    if (j) { // if the head is not NULL
        q->head = j->next; // then we set the head to the next job
        if (!q->head)
            q->tail = NULL; // if the head is NULL, set the tail to NULL
        q->active += 1; // incrementing the active flag
    }
    pthread_mutex_unlock(&q->mtx); // unlock the mutex
    return j; // return the job
}

static void queue_task_done(queue_t *q) { // marking a task as done
    pthread_mutex_lock(&q->mtx); // lock the mutex
    q->active -= 1; // decrement the active flag
    if (!q->open && !q->head && q->active == 0) // if the queue is closed, the head is NULL, and the active flag is 0....
        pthread_cond_broadcast(&q->cv_done); // then we broadcast the condition variable
    pthread_mutex_unlock(&q->mtx); // unlock the mutex
}

static void queue_wait_all(queue_t *q) { // waiting for all tasks to be done
    pthread_mutex_lock(&q->mtx); // lock the mutex
    while (q->open || q->head || q->active) { // while the queue is open, the head is not NULL, or the active flag is not 0
        pthread_cond_wait(&q->cv_done, &q->mtx); // waiting for the condition variable
    }
    pthread_mutex_unlock(&q->mtx); // unlock the mutex
}

/* --------------------------- worker functions --------------------------- */

typedef struct { // struct for the worker arguments
    queue_t *q; // pointer to the queue
} worker_arg_t; // struct for the worker arguments

static void free_job(job_t *j) { // freeing the job
    if (!j) // if the job is NULL, 
        return; //return
    free(j->path); // freeing the path
    free(j); // and the job
}

static void* worker_main(void *arg) { // worker main function
    worker_arg_t *wa = (worker_arg_t*)arg; // get the worker arguments
    queue_t *q = wa->q; // get the queue
    
    // Each thread gets its own buffers to avoid allocation overhead per job
    unsigned char *buffer_in = malloc(BUFFER_SIZE);
    unsigned char *buffer_out = malloc(BUFFER_SIZE);
    
    if (!buffer_in || !buffer_out) { // if allocation failed
        free(buffer_in); // free what we can
        free(buffer_out);
        return NULL; // exit thread
    }
    
    for (;;) { //keep looping
        job_t *j = queue_pop(q); // pop a job from the queue
        if (!j) //if the job is NULL...
            break; // break

        // load file - using simple fread instead of fseek/ftell for better performance
        FILE *f_in = fopen(j->path, "rb");
        if (f_in) {
            // Use setvbuf for larger buffer to improve I/O performance
            setvbuf(f_in, NULL, _IOFBF, BUFFER_SIZE);
            int nbytes = fread(buffer_in, 1, BUFFER_SIZE, f_in);
            fclose(f_in);

            // Store input size
            results_t *res = q->res;
            if (res && j->index >= 0 && j->index < res->count) {
                res->in_sizes[j->index] = nbytes;
            }

            // zip file using zlib
            z_stream strm;
            memset(&strm, 0, sizeof(strm)); // zero out the stream structure
            int ret = deflateInit(&strm, 9); // initialize with max compression (must match reference)
            if (ret == Z_OK) {
                strm.avail_in = nbytes; // set input size
                strm.next_in = buffer_in; // set input buffer
                strm.avail_out = BUFFER_SIZE; // set output size
                strm.next_out = buffer_out; // set output buffer

                ret = deflate(&strm, Z_FINISH); // compress in one shot
                if (ret == Z_STREAM_END) {
                    // dump zipped file
                    int nbytes_zipped = BUFFER_SIZE - strm.avail_out;
                    
                    results_t *res = q->res;
                    if (res && j->index >= 0 && j->index < res->count) {
                        res->out_bufs[j->index] = malloc(nbytes_zipped);
                        if (res->out_bufs[j->index]) {
                            memcpy(res->out_bufs[j->index], buffer_out, nbytes_zipped);
                            res->out_sizes[j->index] = nbytes_zipped;
                        }
                    }
                }
                deflateEnd(&strm); // clean up zlib stream
            }
        }
        
        queue_task_done(q); // mark the task as done
        free_job(j); // free the job
    }
    
    // free thread-local buffers before exiting
    free(buffer_in);
    free(buffer_out);
    return NULL; // return NULL
}

/* ------------------------------ helper functions ----------------------------- */
// ensures we enumerate exactly the .txt files the starter code expects
static int ends_with_txt(const char *s) {
    size_t n = strlen(s);
    // if the length of the string is greater than or equal to 4, ends with ., t, x, t
    return n >= 4 && s[n - 4] == '.' && s[n - 3] == 't' && s[n - 2] == 'x' && s[n - 1] == 't'; 
}

static int cmp_lex(const void *a, const void *b) { // comparing the two strings lexicographically
    const char * const *pa = (const char * const*)a; // get the pointer to the first string
    const char * const *pb = (const char * const*)b; // get the pointer to the second string
    return strcmp(*pa, *pb); // compare the two strings lexicographically
}

/* ------------------------- worker count helper functions ------------------------- */
static int num_cores(void) { // find how many CPU cores are available
#if defined(_WIN32) // if Windows, 
    return 8; //just use a default guess
#else //otherwise...
    long cores = sysconf(_SC_NPROCESSORS_ONLN); // ask the system for core count
    if (cores < 1) return 8; // if something goes wrong, use the default
    if (cores > MAX_WORKER_THREADS) cores = MAX_WORKER_THREADS; // limit it to our max thread count
    return (int)cores; // return the core count as an int
#endif
}

/* ----------------------------- public entry ----------------------------- */
void compress_directory(char *directory_name) { // compressing the directory
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
            dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
            files = realloc(files, (nfiles + 1) * sizeof(char *));
            if (!files) {
                closedir(d);
                return;
            }
            files[nfiles] = strdup(dir->d_name);
            if (!files[nfiles]) {
                closedir(d);
                return;
            }
            nfiles++;
        }
    }
    closedir(d);
    
    if (nfiles == 0) { // if no files found
        free(files); // free and exit
        return;
    }
    
    qsort(files, nfiles, sizeof(char *), cmp_lex); // sort files lexicographically

    results_t res = {0}; // make a results struct and set everything inside to 0
    res.count = nfiles; // remember how many files we'll be working with
    
    // allocating space for arrays to hold compression results
    res.out_bufs = calloc(nfiles, sizeof(unsigned char*));    // storing compressed file data
    res.out_sizes = calloc(nfiles, sizeof(int)); // stores sizes of compressed data
    res.in_sizes = calloc(nfiles, sizeof(int));   // storing sizes of original files
    
    if (!res.out_bufs || !res.out_sizes || !res.in_sizes) {     // checking if any allocation failed (returned NULL)
        for (int i = 0; i < nfiles; i++) // free each filename string
            free(files[i]); 
        free(files);       // free the list of filenames
        free(res.out_bufs);  // and buffers if they were allocated
        free(res.out_sizes);  // free output size array
        free(res.in_sizes);   // free input size array
        return;     // exit early because memory allocation failed
    }

    // making a queue for jobs
    queue_t q; // create a queue struct
    queue_init(&q, &res); // set up its locks and condition variables

    // choose the number of worker threads based on available CPU cores
    int nthreads = num_cores();
    // Use more aggressive threading - oversubscribe slightly for I/O bound work
    if (nfiles > 8) {
        nthreads = nthreads + (nthreads / 2); // use 1.5x cores for many files
    }
    if (nfiles < nthreads) nthreads = nfiles; // don't create more threads than files
    if (nthreads > MAX_WORKER_THREADS) nthreads = MAX_WORKER_THREADS; // respect the limit
    if (nthreads < 1) nthreads = 1; // ensure at least one thread
    
    pthread_t *workers = calloc(nthreads, sizeof(pthread_t)); // allocate memory for the threads
    if (!workers) nthreads = 0; // if allocation failed, fall back to 0 threads
    
    worker_arg_t wa = { .q = &q }; // initialize the worker arguments

    int started = 0; // initialize the number of started threads to 0
    for (int i = 0; i < nthreads; i++) { // create the threads
        if (pthread_create(&workers[i], NULL, worker_main, &wa) != 0) // if the thread creation fails, break
            break;
        started += 1; // increment the number of started threads
    }

    // push all jobs into the queue
    for (int i = 0; i < nfiles; i++) {
        job_t *j = calloc(1, sizeof(job_t)); // allocate a job
        if (!j) continue; // if we cannot allocate a job, skip this file
        
        int len = strlen(directory_name) + strlen(files[i]) + 2;
        j->path = malloc(len * sizeof(char));
        if (!j->path) { // if path allocation failed
            free(j); // free the job
            continue; // move to the next file
        }
        
        strcpy(j->path, directory_name); // copy directory
        strcat(j->path, "/"); // add slash
        strcat(j->path, files[i]); // add filename
        j->index = i; // remember the file's sorted position
        
        queue_push(&q, j); // hand the job to workers
    }

    queue_close(&q); // no more jobs will be added
    queue_wait_all(&q); // wait until the queue is empty and workers are idle

    for (int i = 0; i < started; i++) // join all started threads
        pthread_join(workers[i], NULL); // wait for worker i to finish
    
    free(workers); // free the thread handle array

    // create a single zipped package with all text files in lexicographical order
    FILE *f_out = fopen("text.tzip", "wb"); // open the output file in binary mode
    if (!f_out) { // if the output file does not exist, return
        for (int i = 0; i < nfiles; i++) {
            free(files[i]); // free filename string
            free(res.out_bufs[i]); // free compressed buffer
        }
        free(files);
        free(res.out_bufs);
        free(res.out_sizes);
        free(res.in_sizes);
        return;
    }
    
    int total_in = 0;  // total uncompressed bytes across all files
    int total_out = 0; // total compressed bytes across all files
    for (int i = 0; i < nfiles; i++) { // write each file's result in lex order
        if (res.out_bufs[i] && res.out_sizes[i] > 0) { // only if we have compressed data
            fwrite(&res.out_sizes[i], sizeof(int), 1, f_out);  // write size header
            fwrite(res.out_bufs[i], 1, res.out_sizes[i], f_out); // write compressed bytes
            total_in += res.in_sizes[i];  // add original size
            total_out += res.out_sizes[i]; // add compressed size
        }
    }
    fclose(f_out); // finish the output file

    if (total_in > 0) { // avoid divide-by-zero if there were no bytes
        printf("Compression rate: %.2lf%%\n", 100.0 * (total_in - total_out) / (double)total_in); // print percent saved
    }

    // cleanup section //
    for (int i = 0; i < nfiles; i++) { // free per-file allocations
        free(files[i]); // free filename string
        free(res.out_bufs[i]); // free compressed buffer
    }
    free(files); // free filename list
    free(res.out_bufs); // and array of buffer pointers
    free(res.out_sizes);// and array of compressed sizes
    free(res.in_sizes); // lastly, free array of original sizes
}
