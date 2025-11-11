// Course: COP 4600 - Operating Systems
// Group: 27
// Members:
//     Paige LeClair -  leclair1@usf.edu
//     Laura Robayo  – Laurarobayo@usf.edu@usf.edu
//     Nusraat Kabir - nkabir@usf.edu

// Description:
// we parallelized the baseline compressor using pthreads to speed up
// text file compression. the program scans the directory for .txt files,
// sorts them lexicographically, and pushes one job per file into a
// synchronized queue. worker threads read and deflate files concurrently,
// storing results in indexed buffers. the main thread writes all
// compressed data sequentially into text.tzip to preserve order.
#define _GNU_SOURCE
#include "serial.h"

#include <pthread.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#if !defined(_WIN32)
#include <unistd.h>
#endif
#include <zlib.h>

#define DEFAULT_WORKER_GUESS 4 // default number of worker threads
#ifndef MAX_WORKER_THREADS
#define MAX_WORKER_THREADS 19
#endif


/* -------------------------------overview-------------------------------------------
   1) list *.txt files in lexicographic order
   2) push one job per file into a shared queue
   3) workers read + compress each file
   4) main writes [uint32 size][bytes] for every file into text.tzip

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
    size_t         *out_sizes; // compressed sizes (bytes) 
    size_t         *in_sizes;  // original sizes (bytes) 
    int             count;     //total number of files 
} results_t;

/* ---------------------------- work queue core --------------------------- */

typedef struct {
    pthread_mutex_t mtx;      // protects everything below 
    pthread_cond_t  not_empty;  // signaled when a new job arrives 
    pthread_cond_t  all_done;  // signaled when work is fully finished 
    job_t *head;              //first job in the list 
    job_t *tail;              // last job in the list 
    int    open;              // 1 while producer may still push jobs 
    int    active;            // number of workers currently processing 
} queue_t;

static void queue_init(queue_t *q) { // initializing the the queue
    pthread_mutex_init(&q->mtx, NULL);        // lock protects shared data
    pthread_cond_init(&q->not_empty, NULL);   // signals when a job is added
    pthread_cond_init(&q->all_done, NULL);    // signals when all work is done
    q->head   = NULL;           // no first job yet
    q->tail   = NULL;         // no last job yet
    q->open   = 1;            // still accepting new jobs
    q->active = 0;            // no workers currently busy
}

static void queue_close(queue_t *q) { // closing the queue
    pthread_mutex_lock(&q->mtx); // locking the mutex
    q->open = 0; // setting the open flag to 0
    pthread_cond_broadcast(&q->not_); // broadcast the condition variable
    pthread_mutex_unlock(&q->mtx); // unlocking the mutex
}

static void queue_push(queue_t *q, job_t *j) { // pushing a job to the queue
    j->next = NULL; // setting the next pointer to NULL
    pthread_mutex_lock(&q->mtx); // locking the mutex
    if (q->tail) { // if the tail is not NULL
        q->tail->next = j; // then we set the next pointer of the tail to the job
    } else { // if the tail IS NULL
        q->head = j; // then we set the head to the job
    }
    q->tail = j; // setting the tail to the job
    pthread_cond_signal(&q->not_); // signalling the condition variable
    pthread_mutex_unlock(&q->mtx); // unlocking the mutex
}

static job_t* queue_pop(queue_t *q) { // popping a job from the queue
    pthread_mutex_lock(&q->mtx); // lock the mutex
    while (!q->head && q->open) { // while the head is NULL and the queue is open
        pthread_cond_wait(&q->not_, &q->mtx); // we wait for the condition variable
    }
    job_t *j = q->head; // getting the head of the queue
    if (j) { // if the head is not NULL
        q->head = j->next; // then we set the head to the next job
        if (!q->head){
            q->tail = NULL; // if the head is NULL, set the tail to NULL
        } 
        q->active += 1; // incrementing the active flag
    }
    pthread_mutex_unlock(&q->mtx); // unlock the mutex
    return j; // return the job
}

static void queue_task_done(queue_t *q) { // marking a task as done
    pthread_mutex_lock(&q->mtx); // lock the mutex
    q->active -= 1; // decrement the active flag
    if (!q->open && !q->head && q->active == 0) { // if the queue is closed, the head is NULL, and the active flag is 0....
        pthread_cond_broadcast(&q->all_done); // then we broadcast the condition variable
    }
    pthread_mutex_unlock(&q->mtx); // unlock the mutex
}

static void queue_wait_all(queue_t *q) { // waiting for all tasks to be done
    pthread_mutex_lock(&q->mtx); // lock the mutex
    while (q->open || q->head || q->active) { // while the queue is open, the head is not NULL, or the active flag is not 0
        pthread_cond_wait(&q->all_done, &q->mtx); // waiting for the condition variable
    }
    pthread_mutex_unlock(&q->mtx); // unlock the mutex
}

/* ------------------------------ helper functions ----------------------------- */
// ensures we enumerate exactly the .txt files the starter code expects
static int ends_with_txt(const char *s) {
    size_t n = strlen(s);
    return n >= 4 && // if the length of the string is greater than or equal to 4
           s[n - 4] == '.' && // if the string ends with .
           s[n - 3] == 't' && // if the string ends with t
           s[n - 2] == 'x' && // if the string ends with x
           s[n - 1] == 't'; // if the string ends with t
}

static char* join_path(const char *dir, const char *base) { // joining the path of the file
    size_t a = strlen(dir); // getting the length of the directory
    size_t b = strlen(base); // and the length of the base
    int needs_slash = (a > 0 && dir[a - 1] != '/'); // if the directory does not end with a slash
    char *out = (char*)malloc(a + needs_slash + b + 1); // allocate memory for the output
    if (!out){
         return NULL; // if the memory allocation fails, then... 
    } 
    memcpy(out, dir, a); // copying the directory to the output
    size_t pos = a; // get the position of the output
    if (needs_slash) out[pos++] = '/'; // if the directory does not end with a slash, add a slash
    memcpy(out + pos, base, b); // copy the base to the output
    out[pos + b] = '\0'; // add a null terminator to the output
    return out; // return the output
}

// here we read the entire file into a malloc buffer
static int read_whole_file(const char *path, unsigned char **buf, size_t *len) {
    *buf = NULL; // setting the buffer to NULL
    *len = 0; // and the length to 0
    FILE *f = fopen(path, "rb");      // opening the file for reading 
    if (!f){ // if file couldnt be opened..
         return -1; 
    if (fseek(f, 0, SEEK_END) != 0) {  // move to the end to find its size
        fclose(f); // and close the file
        return -1; 
    }
    long sz = ftell(f);   // get the file size in bytes
    if (sz < 0) {   // if getting the file size failed
        fclose(f); // close the file and return an error
        return -1; 
    }
     rewind(f);  // go back to the start of the file

    if (sz == 0) {     // if file empty
        fclose(f); //close the file
        return 0; // and buf=NULL, len=0
    }
    unsigned char *tmp = (unsigned char*)malloc((size_t)sz);  
    if (!tmp) {   // if memory allocation failed
        fclose(f);   // close the file and return an error
        return -1;  
    }
    size_t off = 0;   // setting ounter for how many bytes have been read so far to 0
    while (off < (size_t)sz) {  // read data from file into buffer until we've read all bytes
        size_t n = fread(tmp + off, 1, (size_t)sz - off, f);  
        if (n == 0) {  // if nothing was read  
            if (ferror(f)) { // and it's a read error,
                free(tmp);   //free
                fclose(f);  // and close
                return -1;  
            }  
            break;  // reached end of file
        }  
        off += n;  // update how much we've read so far
    }
    fclose(f);  // close the file when done
    
    *buf = tmp; // storing the pointer to the loaded data
    *len = off; // store the number of bytes read
    return 0;   
}

static int cmp_lex(const void *a, const void *b) { // comparing the two strings lexicographically
    const char * const *pa = (const char * const*)a; // get the pointer to the first string
    const char * const *pb = (const char * const*)b; // get the pointer to the second string
    return strcmp(*pa, *pb); // compare the two strings lexicographically
}

static char** list_txt_lex(const char *dir, int *out_count) { // listing the txt files lexicographically
    DIR *d = opendir(dir); // opening the directory
    if (!d) {
        *out_count = 0; // set the output count to 0
        return NULL; // return NULL
    }   
    size_t cap = 32; // initialize the capacity to 32
    size_t n = 0; // initialize the number of files to 0
    char **names = (char**)malloc(cap * sizeof(char*)); // allocate memory for the names
    if (!names) { // if the memory allocation fails, return NULL
        closedir(d); // closing the directory
        *out_count = 0; // set the output count to 0
        return NULL; // return NULL
    }

    struct dirent *ent; // pointer to the directory entry
    while ((ent = readdir(d)) != NULL) {
        if (!ends_with_txt(ent->d_name)) continue;
        if (n == cap) { // if the number of files is equal to the capacity
            cap *= 2; // double the capacity
            char **grown = (char**)realloc(names, cap * sizeof(char*)); 
            if (!grown) { // if the memory allocation fails, return NULL
                for (size_t i = 0; i < n; ++i) free(names[i]);
                free(names); // free the names
                closedir(d); // closing the directory
                *out_count = 0; // set the output count to 0
                return NULL; // return NULL
            }
            names = grown; // set the names to the grown names
        }
        names[n] = strdup(ent->d_name); // duplicate the file name
        if (!names[n]) { // if the memory allocation fails, return NULL
            for (size_t i = 0; i < n; ++i) free(names[i]); // free the names
            free(names); // free the names
            closedir(d); // closing the directory
            *out_count = 0; // set the output count to 0
            return NULL; // return NULL
        }
        n += 1; // increment the number of files
    }
    closedir(d); // closing the directory
    qsort(names, n, sizeof(char*), cmp_lex); // sorting the names lexicographically
    *out_count = (int)n; // set the output count to the number of files
    return names; // return the names
}

// here we deflate using zlib header+trailer, mirroring the starter code
static int deflate_buffer(const unsigned char *in, size_t in_len,
                          unsigned char **out, size_t *out_len) {
    if (!out || !out_len) return -1; // if the output or output length is NULL, return -1

    z_stream strm;
    memset(&strm, 0, sizeof(strm)); // memset the stream to 0
    if (deflateInit(&strm, 9) != Z_OK) return -1; // if the initialization fails, return -1

    size_t cap = in_len ? in_len + in_len / 10 + 64 : 64; // calculate the capacity
    unsigned char *dst = (unsigned char*)malloc(cap); // allocate memory for the destination
    if (!dst) { // if the memory allocation fails, end the stream and return -1
        deflateEnd(&strm); // end the stream
        return -1; // return -1
    }

    strm.next_in = (unsigned char*)in; // set the next input to the input
    strm.avail_in = (unsigned int)in_len; // set the available input to the input length
    strm.next_out = dst; // set the next output to the destination
    strm.avail_out = (unsigned int)cap; // set the available output to the capacity

    for (;;) {
        if (strm.avail_out == 0) {
            size_t used = strm.total_out;
            cap *= 2;
            unsigned char *grown = (unsigned char*)realloc(dst, cap); 
            if (!grown) { // if the memory allocation fails, free the destination and end the stream and return -1
                free(dst); // free the destination
                deflateEnd(&strm); // end the stream
                return -1; // return -1
            }
            dst = grown; // set the destination to the grown destination
            strm.next_out = dst + used; // set the next output to the destination + used
            strm.avail_out = (unsigned int)(cap - used); // set the available output to the capacity - used
        }
        int ret = deflate(&strm, Z_FINISH); // deflate the stream
        if (ret == Z_STREAM_END) break; // if the stream ends, break
        if (ret != Z_OK) {
            free(dst); // free the destination
            deflateEnd(&strm); // end the stream
            return -1; // return -1
        }
    }

    *out_len = strm.total_out; // set the output length to the total output length
    *out = dst; // set the output to the destination
    deflateEnd(&strm); // end the stream
    return 0; // return 0
}

/* --------------------------- worker functions --------------------------- */

typedef struct { // struct for the worker arguments
    queue_t   *q; // pointer to the queue
    results_t *res; // pointer to the results
} worker_arg_t; // struct for the worker arguments

static void free_job(job_t *j) { // freeing the job
    if (!j) return; // if the job is NULL, return
    free(j->path); // free the path
    free(j); // free the job
}

static void process_job(job_t *j, results_t *res) {
    unsigned char *raw = NULL;
    size_t raw_len = 0;
    if (read_whole_file(j->path, &raw, &raw_len) != 0) {
        // hard-fail rather than writing a bogus 0-sized member
        //fprintf(stderr, "failed to read %s\n", j->path);
        res->in_sizes[j->index]  = 0;
        res->out_sizes[j->index] = 0;
        res->out_bufs[j->index]  = NULL;
        return;
    }

    unsigned char *cmp = NULL;
    size_t cmp_len = 0;
    if (deflate_buffer(raw, raw_len, &cmp, &cmp_len) != 0) {
        //fprintf(stderr, "deflate failed for %s\n", j->path);
        res->in_sizes[j->index]  = raw_len;
        res->out_sizes[j->index] = 0;
        res->out_bufs[j->index]  = NULL;
        free(raw);
        return;
    }

    res->in_sizes[j->index]  = raw_len;
    res->out_sizes[j->index] = cmp_len;
    res->out_bufs[j->index]  = cmp;
    free(raw);
}


static void* worker_main(void *arg) { // worker main function
    worker_arg_t *wa = (worker_arg_t*)arg; // get the worker arguments
    queue_t *q = wa->q; // get the queue
    results_t *res = wa->res; // get the results

    for (;;) {
        job_t *j = queue_pop(q); // pop a job from the queue
        if (!j) break; // if the job is NULL, break
        process_job(j, res); // process the job
        free_job(j); // free the job
        queue_task_done(q); // mark the task as done
    }
    return NULL; // return NULL
}

static void process_file_direct(const char *dir, const char *name, // processing the file directly
                                int index, results_t *res) { // processing the file directly
    job_t tmp = {0}; // initialize the job to 0
    tmp.path = join_path(dir, name); // join the path of the file
    if (!tmp.path) { // if the path is NULL, return
        res->in_sizes[index] = 0; // set the input size to 0
        res->out_sizes[index] = 0; // set the output size to 0
        res->out_bufs[index] = NULL; // set the output buffer to NULL
        return; // return
    }
    tmp.index = index; // set the index to the index
    process_job(&tmp, res); // process the job
    free(tmp.path); // free the path
}

/* ------------------------- worker count helpers ------------------------- */

static int clamp_threads(int n) { // clamping the threads
    if (n < 1) n = 1; // if the number of threads is less than 1, set the number of threads to 1
    if (n > MAX_WORKER_THREADS) n = MAX_WORKER_THREADS; // if the number of threads is greater than the maximum number of threads, set the number of threads to the maximum number of threads
    return n; // return the number of threads
}

static int num_cores(void) { // getting the number of hardware threads
#if defined(_WIN32) // if the operating system is Windows
    return DEFAULT_WORKER_GUESS; // return the default number of threads
#else
    long cores = sysconf(_SC_NPROCESSORS_ONLN); // get the number of cores
    if (cores < 1) return DEFAULT_WORKER_GUESS; // if the number of cores is less than 1, set the number of cores to the default number of threads
    if (cores > MAX_WORKER_THREADS) cores = MAX_WORKER_THREADS; // if the number of cores is greater than the maximum number of threads, set the number of cores to the maximum number of threads
    return (int)cores; // return the number of cores
#endif
}

static int pick_workers(int jobs) {
    if (jobs <= 1) return 1;
    int cores = num_cores();
    int want = 2 * (cores > 0 ? cores : DEFAULT_WORKER_GUESS);
    if (want > jobs) want = jobs;
    if (want > MAX_WORKER_THREADS) want = MAX_WORKER_THREADS;
    return want;
}


/* ----------------------------- public entry ----------------------------- */

void compress_directory(char *directory_name) { // compressing the directory
    int nfiles = 0; // initialize the number of files to 0
    char **files = list_txt_lex(directory_name, &nfiles); // list the txt files lexicographically

    FILE *f_out = fopen("text.tzip", "wb"); // open the output file in binary mode
    if (!f_out) { // if the output file does not exist, return
        for (int i = 0; i < nfiles; ++i) free(files[i]); // free the files
        free(files); // free the files
        return; // return
    }

    if (!files || nfiles == 0) { // if the files are NULL or the number of files is 0, return
        fclose(f_out); // close the output file
        if (files) { // if the files are not NULL
            for (int i = 0; i < nfiles; ++i) free(files[i]); // free the files
            free(files); // free the files
        }
        return; // return
    }

    results_t res = {0};
    res.count = nfiles; // set the count to the number of files
    res.out_bufs  = (unsigned char**)calloc((size_t)nfiles, sizeof(unsigned char*)); // allocate memory for the output buffers
    res.out_sizes = (size_t*)calloc((size_t)nfiles, sizeof(size_t)); // allocate memory for the output sizes    
    res.in_sizes  = (size_t*)calloc((size_t)nfiles, sizeof(size_t)); // allocate memory for the input sizes
    if (!res.out_bufs || !res.out_sizes || !res.in_sizes) { // if the output buffers, output sizes, or input sizes are NULL, return
        fclose(f_out); // close the output file
        for (int i = 0; i < nfiles; ++i) free(files[i]); // free the files
        free(files); // free the files
        free(res.out_bufs); // free the output buffers
        free(res.out_sizes); // free the output sizes
        free(res.in_sizes); // free the input sizes
        return; // return
    }

    queue_t q; // initialize the queue
    queue_init(&q); // initialize the queue

    int nthreads_target = pick_workers(nfiles); // choose the number of worker threads
    pthread_t *ths = (pthread_t*)calloc((size_t)nthreads_target, sizeof(pthread_t)); // allocate memory for the threads
    worker_arg_t wa = { .q = &q, .res = &res }; // initialize the worker arguments

    int started = 0; // initialize the number of started threads to 0
    if (ths) {
        for (int i = 0; i < nthreads_target; ++i) { // create the threads
            if (pthread_create(&ths[i], NULL, worker_main, &wa) != 0) { // if the thread creation fails, break
                break;
            }
            started += 1; // increment the number of started threads
        }
    }

    if (started == 0) {
        for (int i = 0; i < nfiles; ++i) { // process the files directly
            process_file_direct(directory_name, files[i], i, &res); // process the file directly
        }
    } else {
        for (int i = 0; i < nfiles; ++i) { // process the files
            job_t *j = (job_t*)calloc(1, sizeof(job_t)); // allocate memory for the job
            if (!j) { // if the job is NULL, process the file directly
                process_file_direct(directory_name, files[i], i, &res); // process the file directly
                continue; // continue to the next file
            }
            j->path = join_path(directory_name, files[i]);
            if (!j->path) { // if the path is NULL, free the job and process the file directly
                free_job(j); // free the job
                process_file_direct(directory_name, files[i], i, &res); // process the file directly
                continue;
            }
            j->index = i; // set the index to the index
            queue_push(&q, j); // push the job to the queue
        }

        queue_close(&q); // close the queue
        queue_wait_all(&q); // wait for all tasks to be done
        for (int i = 0; i < started; ++i) { // join the threads
            pthread_join(ths[i], NULL); // join the thread
        }
    }
    free(ths); // free the threads

    size_t total_in = 0; // initialize the total input size to 0
    size_t total_out = 0; // initialize the total output size to 0
    for (int i = 0; i < nfiles; ++i) { // write the output buffers to the output file
        if (res.out_bufs[i] && res.out_sizes[i] > 0) {
            uint32_t stored = (uint32_t)res.out_sizes[i]; // seting the stored to the output size
            fwrite(&stored, sizeof(uint32_t), 1, f_out); // writing the stored to the output file
            fwrite(res.out_bufs[i], 1, res.out_sizes[i], f_out); // writing the output buffer to the output file
            total_in  += res.in_sizes[i]; // increment the total input size by the input size
            total_out += res.out_sizes[i]; // increment the total output size by the output size
        }
    }
    fclose(f_out); // close the output file

    if (total_in > 0) { // if the total input size is greater than 0, print the compression rate
        double saved = (double)(total_in - total_out) / (double)total_in; // calculate the compression rate
        printf("Compression rate: %.2lf%%\n", saved * 100.0); // print the compression rate
    }

    /* cleanup */
    for (int i = 0; i < nfiles; ++i) { // free the files and the output buffers and the output sizes and the input sizes
        free(files[i]); // free the file
        free(res.out_bufs[i]); // free the output buffer
    }
    free(files); // free the files
    free(res.out_bufs); // free the output buffers
    free(res.out_sizes); // free the output sizes
    free(res.in_sizes); // free the input sizes
}