///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Course: COP 4600 
// Group: 4
// Members:
//
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

#define _GNU_SOURCE
#include "serial.h"

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

#ifndef MAX_WORKER_THREADS // ensures we always have a max threasd limit
#define MAX_WORKER_THREADS 19   // cap total threads to 19 (plus main thread = 20 max)
#endif

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
    pthread_mutex_t res_mtx;  // ADDED: protecting results array writes so threads don't step on each other
} results_t;

/* ---------------------------- work queue core --------------------------- */
typedef struct {
    pthread_mutex_t mtx;      // protects everything below 
    pthread_cond_t cv_have;  // signaled when a new job arrives 
    pthread_cond_t cv_done;  // signaled when work is fully finished 
    job_t *head;              //first job in the list 
    job_t *tail;              // last job in the list 
    int open;              // 1 while producer may still push jobs 
    int active;            // number of workers currently processing 
    results_t *res;        // ADDED: storing a pointer to results so workers can access it
} queue_t;

static void queue_init(queue_t *q, results_t *res) { // initializing the the queue (NOW takes results pointer)
    pthread_mutex_init(&q->mtx, NULL);        // lock protects shared data
    pthread_cond_init(&q->cv_have, NULL);   // signals when a job is added
    pthread_cond_init(&q->cv_done, NULL);    // signals when all work is done
    q->head = NULL;           // no first job yet
    q->tail = NULL;         // no last job yet
    q->open = 1;            // still accepting new jobs
    q->active = 0;            // no workers currently busy
    q->res = res;           // ADDED: store results pointer in queue
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
    if (q->tail) { // if the tail is not NULL
        q->tail->next = j; // then we set the next pointer of the tail to the job
    } 
    else { // if the tail IS NULL
        q->head = j; // then we set the head to the job
    }
    q->tail = j; // setting the tail to the job
    pthread_cond_signal(&q->cv_have); // signalling the condition variable
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
        pthread_cond_broadcast(&q->cv_done); // then we broadcast the condition variable
    }
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
} worker_arg_t; // CHANGED: removed res pointer since queue now holds it

static void free_job(job_t *j) { // freeing the job
    if (!j) { // if the job is NULL, 
        return; //return
    }
    free(j->path); // freeing the path
    free(j); // and the job
}

static void* worker_main(void *arg) { // worker main function
    worker_arg_t *wa = (worker_arg_t*)arg; // get the worker arguments
    queue_t *q = wa->q; // get the queue
    
    for (;;) { //keep looping
        job_t *j = queue_pop(q); // pop a job from the queue
        if (!j) { //if the job is NULL...
            break; // break
        }

        // Open and get file size
        FILE *f_in = fopen(j->path, "rb"); // opening the file for reading 
        if (!f_in) { // if file couldnt be opened..
            queue_task_done(q); // mark the task as done
            free_job(j); // free the job
            continue; // skip to the next job
        }
        
        fseek(f_in, 0, SEEK_END); // move to the end to find its size
        long file_size = ftell(f_in); // get the file size in bytes
        rewind(f_in); // go back to the start of the file
        
        if (file_size < 0) { // if getting the file size failed
            fclose(f_in); // close the file
            queue_task_done(q); // mark the task as done
            free_job(j); // free the job
            continue; // skip to the next job
        }
        
        // Allocate input buffer
        unsigned char *buffer_in = malloc(file_size > 0 ? file_size : 1); // allocate memory for the input buffer
        if (!buffer_in) { // if memory allocation failed
            fclose(f_in); // close the file
            queue_task_done(q); // mark the task as done
            free_job(j); // free the job
            continue; // skip to the next job
        }
        
        // Read file
        size_t bytes_read = 0; // setting counter for how many bytes have been read so far to 0
        if (file_size > 0) { // if the file has content
            bytes_read = fread(buffer_in, 1, file_size, f_in); // read data from file into buffer
        }
        fclose(f_in); // close the file when done
        
        // Store input size (with mutex protection)
        results_t *res = q->res; // CHANGED: getting results from queue instead of worker_arg
        if (res && j->index >= 0 && j->index < res->count) { // make sure results pointer is valid and index is in range
            pthread_mutex_lock(&res->res_mtx); // lock the results mutex so only one thread writes at a time
            res->in_sizes[j->index] = bytes_read; // storing size before compression
            pthread_mutex_unlock(&res->res_mtx); // unlock the results mutex
        }
        
        // Handle empty files
        if (bytes_read == 0) { // if file is empty
            free(buffer_in); // free the input buffer
            queue_task_done(q); // mark the task as done
            free_job(j); // free the job
            continue; // skip to the next job
        }
        
        // Allocate output buffer (initial estimate)
        size_t out_cap = bytes_read + bytes_read / 10 + 64; // calculate the capacity for compressed data
        unsigned char *buffer_out = malloc(out_cap); // allocate memory for the output buffer
        if (!buffer_out) { // if memory allocation failed
            free(buffer_in); // free the input buffer
            queue_task_done(q); // mark the task as done
            free_job(j); // free the job
            continue; // skip to the next job
        }
        
        // Compress
        z_stream strm; // zlib compression stream
        memset(&strm, 0, sizeof(strm)); // set all bytes in the struct to 0
        if (deflateInit(&strm, 9) != Z_OK) { // tries to start zlib compression with level 9 (best compression)
            free(buffer_in); // free the input buffer
            free(buffer_out); // free the output buffer
            queue_task_done(q); // mark the task as done
            free_job(j); // free the job
            continue; // skip to the next job
        }
        
        strm.avail_in = (unsigned int)bytes_read; // set the available input to the bytes read
        strm.next_in = buffer_in; // set the next input to the input buffer
        strm.avail_out = (unsigned int)out_cap; // set the available output to the capacity
        strm.next_out = buffer_out; // set the next output to the output buffer
        
        // Compress with dynamic growth
        int compress_ok = 1; // flag to track if compression succeeded
        for (;;) { // keep looping until compression finishes
            if (strm.avail_out == 0) { // if the output buffer is full
                size_t used = strm.total_out; // record how many bytes have been written so far
                out_cap *= 2; // double the buffer size to make more room
                unsigned char *grown = realloc(buffer_out, out_cap); // try to grow the buffer
                if (!grown) { // if the memory allocation fails
                    compress_ok = 0; // mark compression as failed
                    break; // stop compressing
                }
                buffer_out = grown; // setting the output buffer to the grown buffer
                strm.next_out = buffer_out + used; // set the next output to the buffer + used
                strm.avail_out = (unsigned int)(out_cap - used); // set the available output to the capacity - used
            }
            
            int ret = deflate(&strm, Z_FINISH); // deflate the stream
            if (ret == Z_STREAM_END){ // if the stream ends, 
                break; //then break
            }
            if (ret != Z_OK) { // if zlib didn't return Z_OK then something went wrong
                compress_ok = 0; // mark compression as failed
                break; // stop compressing
            }
        }
        
        // Save result (with mutex protection)
        if (compress_ok && res && j->index >= 0 && j->index < res->count) { // if compression worked and results is valid
            size_t nbytes_zipped = strm.total_out; // get the total compressed size
            unsigned char *final_buf = malloc(nbytes_zipped); // allocate a properly sized buffer for the compressed data
            
            pthread_mutex_lock(&res->res_mtx); // lock the results mutex so only one thread writes at a time
            if (final_buf) { // if we got the memory
                memcpy(final_buf, buffer_out, nbytes_zipped); // copy compressed data to final buffer
                res->out_bufs[j->index] = final_buf; // storing pointer to compressed data
                res->out_sizes[j->index] = nbytes_zipped; // storing size after compression
            }
            pthread_mutex_unlock(&res->res_mtx); // unlock the results mutex
        }
        
        deflateEnd(&strm); // ending the stream
        free(buffer_out); // free the output buffer
        free(buffer_in); // free the input buffer
        
        queue_task_done(q); // mark the task as done
        free_job(j); // free the job
    }
    
    return NULL; // return NULL
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

static int cmp_lex(const void *a, const void *b) { // comparing the two strings lexicographically
    const char * const *pa = (const char * const*)a; // get the pointer to the first string
    const char * const *pb = (const char * const*)b; // get the pointer to the second string
    return strcmp(*pa, *pb); // compare the two strings lexicographically
}

static int num_cores(void) { // find how many CPU cores are available
#if defined(_WIN32) // if Windows, 
    return 8; //just use 8 as a reasonable default
#else //otherwise...
    long cores = sysconf(_SC_NPROCESSORS_ONLN); // ask the system for core count
    if (cores < 1) return 8; // if something goes wrong, use 8 as default
    if (cores > MAX_WORKER_THREADS) cores = MAX_WORKER_THREADS; // limit it to our max thread count
    return (int)cores; // return the core count as an int
#endif
}

/* ----------------------------- public entry ----------------------------- */
void compress_directory(char *directory_name) { // compressing the directory
    DIR *d;
    struct dirent *dir;
    char **files = NULL; // array to hold filenames
    int nfiles = 0; // count of how many files we found

    d = opendir(directory_name); // opening the directory
    if (d == NULL) { // if the directory does not exist
        printf("An error has occurred\n"); // print error message
        return; // return
    }

    // Create sorted list of text files
    while ((dir = readdir(d)) != NULL) { // loop through directory entries
        if (!ends_with_txt(dir->d_name)) { // if it's not a .txt file
            continue; // skip it
        }
            
        char **temp = realloc(files, (nfiles + 1) * sizeof(char *)); // grow the array by one slot
        if (!temp) { // if realloc failed
            for (int i = 0; i < nfiles; i++) { // free all the strings we've saved so far
                free(files[i]);
            }
            free(files); // free the array itself
            closedir(d); // closing the directory
            return; // bail out
        }
        files = temp; // update our pointer to the grown array
        
        files[nfiles] = strdup(dir->d_name); // copy the filename
        if (!files[nfiles]) { // if strdup failed
            for (int i = 0; i < nfiles; i++) { // free everything
                free(files[i]);
            }
            free(files);
            closedir(d); // closing the directory
            return; // bail out
        }
        nfiles++; // increment file count
    }
    closedir(d); // closing the directory
    
    if (nfiles == 0) { // if no files found
        free(files); // free the (empty) array
        return; // nothing to do
    }
    
    qsort(files, nfiles, sizeof(char *), cmp_lex); // sorting the files lexicographically

    // Initialize results
    results_t res = {0}; // make a results struct and set everything inside to 0
    res.count = nfiles; // remember how many files we'll be working with
    res.out_bufs = calloc(nfiles, sizeof(unsigned char*)); // storeing compressed file data
    res.out_sizes = calloc(nfiles, sizeof(size_t)); // stores sizes of compressed data
    res.in_sizes = calloc(nfiles, sizeof(size_t)); // storing sizes of original files
    pthread_mutex_init(&res.res_mtx, NULL); // ADDED: initialize the mutex that protects results writes
    
    if (!res.out_bufs || !res.out_sizes || !res.in_sizes) { // checking if any allocation failed (returned NULL)
        for (int i = 0; i < nfiles; i++) { // free all filenames
            free(files[i]);
        }
        free(files); // free filename array
        free(res.out_bufs); // and buffers if they were allocated
        free(res.out_sizes); // free output size array
        free(res.in_sizes); // free input size array
        return; // exit early because memory allocation failed
    }

    // Initialize queue
    queue_t q; // create a queue struct
    queue_init(&q, &res); // CHANGED: now passing results pointer to queue_init

    // Determine thread count
    int nthreads = num_cores(); // get number of CPU cores
    if (nfiles > 8) { // if we have a lot of files
        nthreads = nthreads + (nthreads / 2); // use 1.5x the core count for better throughput
    }
    if (nfiles < nthreads) {
        nthreads = nfiles; // don't create more threads than files
    }
    if (nthreads > MAX_WORKER_THREADS) {
        nthreads = MAX_WORKER_THREADS; // cap it at our max
    }
    if (nthreads < 1) {
        nthreads = 1; // at least one thread
    }
    
    pthread_t *workers = calloc(nthreads, sizeof(pthread_t)); // allocate memory for thread handles
    if (!workers) {
        nthreads = 0; // if allocation failed, fall back to zero threads (single-threaded mode)
    }
    
    worker_arg_t wa = { .q = &q }; // initialize the worker arguments (just the queue now)

    // Create threads
    int started = 0; // initialize the number of started threads to 0
    for (int i = 0; i < nthreads; i++) { // create the threads
        if (pthread_create(&workers[i], NULL, worker_main, &wa) != 0) { // FIXED: using &workers[i] instead of *workers[i]
            break; // if thread creation fails, stop trying
        }
        started++; // increment the number of started threads
    }

    // Push all jobs
    for (int i = 0; i < nfiles; i++) { // create one job per file
        job_t *j = calloc(1, sizeof(job_t)); // allocate a job
        if (!j) {
            continue; // if we can't allocate, skip this file (it won't be processed)
        }
        
        size_t len = strlen(directory_name) + strlen(files[i]) + 2; // calculate path length (dir + "/" + file + null terminator)
        j->path = malloc(len); // allocate memory for the path
        if (!j->path) { // if path allocation failed
            free(j); // free the job
            continue; // skip this file
        }
        
        snprintf(j->path, len, "%s/%s", directory_name, files[i]); // build "dir/name"
        j->index = i; // remember the file's sorted position
        
        queue_push(&q, j); // hand the job to workers
    }

    queue_close(&q); // no more jobs will be added
    queue_wait_all(&q); // wait until the queue is empty and workers are idle

    // Join threads
    for (int i = 0; i < started; i++) { // join all started threads
        pthread_join(workers[i], NULL); // wait for worker i to finish
    }
    
    free(workers); // free the thread handle array

    // Write output
    FILE *f_out = fopen("text.tzip", "wb"); // open the output file in binary mode
    if (!f_out) { // if opening output file failed
        for (int i = 0; i < nfiles; i++) { // cleanup everything
            free(files[i]); // free filename
            free(res.out_bufs[i]); // free compressed buffer
        }
        free(files); // free filename array
        free(res.out_bufs); // and array of buffer pointers
        free(res.out_sizes); // and array of compressed sizes
        free(res.in_sizes); // lastly, free array of original sizes
        pthread_mutex_destroy(&res.res_mtx); // ADDED: destroy the results mutex
        return; // bail out
    }
    
    size_t total_in = 0;  // total uncompressed bytes across all files
    size_t total_out = 0; // total compressed bytes across all files
    for (int i = 0; i < nfiles; i++) { // write each file's result in lex order
        if (res.out_bufs[i] && res.out_sizes[i] > 0) { // only if we have compressed data
            uint32_t size_prefix = (uint32_t)res.out_sizes[i]; // 4-byte length prefix (compressed size)
            fwrite(&size_prefix, sizeof(uint32_t), 1, f_out); // write size header
            fwrite(res.out_bufs[i], 1, res.out_sizes[i], f_out); // write compressed bytes
            total_in += res.in_sizes[i]; // add original size
            total_out += res.out_sizes[i]; // add compressed size
        }
    }
    fclose(f_out); // finish the output file

    if (total_in > 0) { // avoid divide-by-zero if there were no bytes
        printf("Compression rate: %.2lf%%\n", 100.0 * (total_in - total_out) / (double)total_in); // print percent saved
    }

    // Cleanup
    for (int i = 0; i < nfiles; i++) { // free per-file allocations
        free(files[i]); // free filename string
        free(res.out_bufs[i]); // free compressed buffer
    }
    free(files); // free filename list
    free(res.out_bufs); // and array of buffer pointers
    free(res.out_sizes); // and array of compressed sizes
    free(res.in_sizes); // lastly, free array of original sizes
    pthread_mutex_destroy(&res.res_mtx); // ADDED: destroy the results mutex when we're all done
}