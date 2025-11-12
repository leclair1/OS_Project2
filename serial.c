///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Course: COP 4600 
// Group: 4
// Members:
//     Paige LeClair -  leclair1@usf.edu
//     Laura Robayo  – Laurarobayo@usf.edu
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

// for this, we only include <unistd.h> if we’re not on Windows (so POSIX-only functions)
#if !defined(_WIN32)
#include <unistd.h>
#endif

#include <zlib.h> // for compression and decompression (zlib library)

// thread configuration constants
#define DEFAULT_WORKER_GUESS 4  // default number of worker threads to try
#ifndef MAX_WORKER_THREADS // ensures we always have a max threasd limit
#define MAX_WORKER_THREADS 19 // cap total threads to 19 (plus main thread = 20 max)
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
    pthread_cond_t not_empty;  // signaled when a new job arrives 
    pthread_cond_t all_done;  // signaled when work is fully finished 
    job_t *head;              //first job in the list 
    job_t *tail;              // last job in the list 
    int open;              // 1 while producer may still push jobs 
    int active;            // number of workers currently processing 
} queue_t;

static void queue_init(queue_t *q) { // initializing the the queue
    pthread_mutex_init(&q->mtx, NULL);        // lock protects shared data
    pthread_cond_init(&q->not_empty, NULL);   // signals when a job is added
    pthread_cond_init(&q->all_done, NULL);    // signals when all work is done
    q->head = NULL;           // no first job yet
    q->tail = NULL;         // no last job yet
    q->open = 1;            // still accepting new jobs
    q->active = 0;            // no workers currently busy
}

static void queue_close(queue_t *q) { // closing the queue
    pthread_mutex_lock(&q->mtx); // locking the mutex
    q->open = 0; // setting the open flag to 0
    pthread_cond_broadcast(&q->not_empty); // broadcast the condition variable
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
    pthread_cond_signal(&q->not_empty); // signalling the condition variable
    pthread_mutex_unlock(&q->mtx); // unlocking the mutex
}

static job_t* queue_pop(queue_t *q) { // popping a job from the queue
    pthread_mutex_lock(&q->mtx); // lock the mutex
    while (!q->head && q->open) { // while the head is NULL and the queue is open
        pthread_cond_wait(&q->not_empty, &q->mtx); // we wait for the condition variable
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
        pthread_cond_broadcast(&q->all_done); // then we broadcast the condition variable
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
    // if the length of the string is greater than or equal to 4, ends with ., t, x, t
    return n >= 4 && s[n - 4] == '.' && s[n - 3] == 't' && s[n - 2] == 'x' && s[n - 1] == 't'; 
}

static char* join_path(const char *dir, const char *base) { // joining the path of the file
    size_t a = strlen(dir); // getting the length of the directory
    size_t b = strlen(base); // and the length of the base
    int needs_slash = (a > 0 && dir[a - 1] != '/'); // if the directory does not end with a slash
    char *out = (char*)malloc(a + needs_slash + b + 1); // allocate memory for the output
    if (!out)
         return NULL; // if the memory allocation fails, then... 
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
    if (!f) // if file couldnt be opened..
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
            for (size_t i = 0; i < n; ++i)
                 free(names[i]); // free the names
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
static int deflate_buffer(const unsigned char *in, size_t in_len, unsigned char **out, size_t *out_len) {
    if (!out || !out_len)  // makes sure the pointers given by the caller aren’t NULL  
        return -1; // if they are, we can’t store the compressed data so return error  
    z_stream strm; 
    memset(&strm, 0, sizeof(strm)); // set all bytes in the struct to 0

    if (deflateInit(&strm, 9) != Z_OK) // tries to start zlib compression with level 9 (best compression)  
        return -1;  // if zlib fails to initialize, return error  
    size_t cap = in_len ? in_len + in_len / 10 + 64 : 64; // calculate the capacity
    unsigned char *dst = (unsigned char*)malloc(cap); // allocate memory for the destination
    if (!dst) {     // if memory allocation for the output buffer failed... 
        deflateEnd(&strm); //free zlib stream
        return -1; // return -1
    }

    strm.next_in = (unsigned char*)in; // set the next input to the input
    strm.avail_in = (unsigned int)in_len; // set the available input to the input length
    strm.next_out = dst; // set the next output to the destination
    strm.avail_out = (unsigned int)cap; // set the available output to the capacity

    for (;;) { // keep looping until compression finishes
        if (strm.avail_out == 0) { // if the output buffer is full
            size_t used = strm.total_out; // record how many bytes have been written so far
            cap *= 2; // double the buffer size to make more room    
            unsigned char *grown = (unsigned char*)realloc(dst, cap); 
            if (!grown) { // if the memory allocation fails, free the destination and end the stream and return -1
                free(dst); // free the destination
                deflateEnd(&strm); // end the stream
                return -1; // return -1
            }
            dst = grown; // setting the destination to the grown destination
            strm.next_out = dst + used; // set the next output to the destination + used
            strm.avail_out = (unsigned int)(cap - used); // set the available output to the capacity - used
        }
        int ret = deflate(&strm, Z_FINISH); // deflate the stream
        if (ret == Z_STREAM_END) // if the stream ends, 
            break; //then break
        if (ret != Z_OK) { // if zlib didn’t return Z_OK then something went wrong
            free(dst); // so we free the memory allocated for this
            deflateEnd(&strm); // end the stream
            return -1; // return -1
        }
    }

    *out_len = strm.total_out; // setting the output length to the total output length
    *out = dst; // and setting the output to the destination
    deflateEnd(&strm); // ending the stream
    return 0; // return 0
}

/* --------------------------- worker functions --------------------------- */

typedef struct { // struct for the worker arguments
    queue_t   *q; // pointer to the queue
    results_t *res; // pointer to the results
} worker_arg_t; // struct for the worker arguments

static void free_job(job_t *j) { // freeing the job
    if (!j) // if the job is NULL, 
        return; //return
    free(j->path); // freeing the path
    free(j); // and the job
}

static void process_job(job_t *j, results_t *res) {
    unsigned char *raw = NULL; // pointer for the file’s uncompressed data
    size_t raw_len = 0; // length of the uncompressed data

    if (read_whole_file(j->path, &raw, &raw_len) != 0) { // read the file into memory
        // if reading failed, mark sizes as 0 so we skip this file
        res->in_sizes[j->index]  = 0;
        res->out_sizes[j->index] = 0;
        res->out_bufs[j->index]  = NULL;
        return; // stop processing this job
    }
    unsigned char *cmp = NULL; // pointer for the compressed data
    size_t cmp_len = 0; // length of the compressed data

    if (deflate_buffer(raw, raw_len, &cmp, &cmp_len) != 0) { // compress the data
        // if compression failed, record the input size but no output
        res->in_sizes[j->index] = raw_len;
        res->out_sizes[j->index] = 0;
        res->out_bufs[j->index] = NULL;
        free(raw); // free the uncompressed buffer
        return; // stop processing this job
    }
    res->in_sizes[j->index] = raw_len;  // storing size before compression
    res->out_sizes[j->index] = cmp_len;  // storing size after compression
    res->out_bufs[j->index] = cmp;   // storing pointer to compressed data
    free(raw); // freeing the uncompressed memory now that it’s not needed
}


static void* worker_main(void *arg) { // worker main function
    worker_arg_t *wa = (worker_arg_t*)arg; // get the worker arguments
    queue_t *q = wa->q; // get the queue
    results_t *res = wa->res; // get the results

    for (;;) { //keep looping
        job_t *j = queue_pop(q); // pop a job from the queue
        if (!j) //if the job is NULL...
            break; // break
        process_job(j, res); // process the job
        free_job(j); // free the job
        queue_task_done(q); // mark the task as done
    }
    return NULL; // return NULL
}

static void process_file_direct(const char *dir, const char *name, int index, results_t *res) { // processing the file directly
    job_t tmp = {0}; // initialize the job to 0
    tmp.path = join_path(dir, name); // join the path of the file
    if (!tmp.path) { // if the path is NULL, return
        res->in_sizes[index] = 0; // setting the input size to 0
        res->out_sizes[index] = 0; // and the output size to 0
        res->out_bufs[index] = NULL; // and setting the output buffer to NULL
        return; // return
    }
    tmp.index = index; // set the index to the index
    process_job(&tmp, res); // process the job
    free(tmp.path); // freeing the path
}

/* ------------------------- worker count helper functions ------------------------- */
static int clamp_threads(int n) { // keep thread count within allowed range
    if (n < 1) // make sure we have at least one thread
        n = 1; 
    if (n > MAX_WORKER_THREADS) // cap it at the maximum limit
        n = MAX_WORKER_THREADS; 
    return n; // return the adjusted number
}

static int num_cores(void) { // find how many CPU cores are available
#if defined(_WIN32) // if Windows, 
    return DEFAULT_WORKER_GUESS; //just use a default guess
#else //otherwise...
    long cores = sysconf(_SC_NPROCESSORS_ONLN); // ask the system for core count
    if (cores < 1) return DEFAULT_WORKER_GUESS; // if something goes wrong, use the default
    if (cores > MAX_WORKER_THREADS) cores = MAX_WORKER_THREADS; // limit it to our max thread count
    return (int)cores; // return the core count as an int
#endif
}

static int pick_workers(int jobs) { // decide how many worker threads to use
    if (jobs <= 1) return 1; // only one thread if there’s one job
    int cores = num_cores(); // get the number of CPU cores
    int want = 2 * (cores > 0 ? cores : DEFAULT_WORKER_GUESS); // usually use double the core count
    if (want > jobs) want = jobs; // don’t start more threads than jobs
    if (want > MAX_WORKER_THREADS) want = MAX_WORKER_THREADS; // cap it again if it’s too high
    return want; // return the final thread count
}

/* ----------------------------- public entry ----------------------------- */
void compress_directory(char *directory_name) { // compressing the directory
    int nfiles = 0; // initializing the number of files to 0
    char **files = list_txt_lex(directory_name, &nfiles); // list the txt files lexicographically

    FILE *f_out = fopen("text.tzip", "wb"); // open the output file in binary mode
    if (!f_out) { // if the output file does not exist, return
        for (int i = 0; i < nfiles; ++i) // free the files
            free(files[i]); 
        free(files); // free the files
        return; // return
    }

    if (!files || nfiles == 0) { // if the files are NULL or the number of files is 0, return
        fclose(f_out); // close the output file
        if (files) { // if the files are not NULL
            for (int i = 0; i < nfiles; ++i) // free the files
                free(files[i]); 
            free(files); // free the files
        }
        return; // return
    }

    results_t res = {0}; // make a results struct and set everything inside to 0
    res.count = nfiles; // remember how many files we’ll be working with
    
    // allocating space for arrays to hold compression results
    res.out_bufs  = (unsigned char**)calloc((size_t)nfiles, sizeof(unsigned char*));    // storeing compressed file data
    res.out_sizes = (size_t*)calloc((size_t)nfiles, sizeof(size_t)); // stores sizes of compressed data
    res.in_sizes  = (size_t*)calloc((size_t)nfiles, sizeof(size_t));   // storing sizes of original files

    if (!res.out_bufs || !res.out_sizes || !res.in_sizes) {     // checking if any allocation failed (returned NULL)
        fclose(f_out);          // if yes, close the output file since we can’t continue
        for (int i = 0; i < nfiles; ++i) // free each filename string
            free(files[i]); 
        free(files);       // free the list of filenames
        free(res.out_bufs);  // and buffers if they were allocated
        free(res.out_sizes);  // free output size array
        free(res.in_sizes);   // free input size array
        return;     // exit early because memory allocation failed
    }
    
    // making a queue for jobs
    queue_t q; // create a queue struct
    queue_init(&q); // set up its locks and condition variables
    

    int nthreads_target = pick_workers(nfiles); // choose the number of worker threads
    pthread_t *workers = (pthread_t*)calloc((size_t)nthreads_target, sizeof(pthread_t)); // allocate memory for the threads
    worker_arg_t wa = { .q = &q, .res = &res }; // initialize the worker arguments

    int started = 0; // initialize the number of started threads to 0
    if (*workers) {
        for (int i = 0; i < nthreads_target; ++i) { // create the threads
            if (pthread_create(&workers[i], NULL, worker_main, &wa) != 0) // if the thread creation fails, break
                break;
            started += 1; // increment the number of started threads
        }
    }

    if (started == 0) { // if no worker threads were created
        for (int i = 0; i < nfiles; ++i)  // we go through every file one by one
            process_file_direct(directory_name, files[i], i, &res); // and process the file directly
    } 
    else { // otherwise, run in parallel with worker threads
        for (int i = 0; i < nfiles; ++i) { // create one job per file
            job_t *j = (job_t*)calloc(1, sizeof(job_t)); // allocate a job
            if (!j) { // if we cannot allocate a job, do this file in the main thread
                process_file_direct(directory_name, files[i], i, &res); // fallback
                continue; // move to the next file
            }
    
            j->path = join_path(directory_name, files[i]); // build "dir/name"
            if (!j->path) { // if path build failed, free job and fallback
                free_job(j); // free the half-built job
                process_file_direct(directory_name, files[i], i, &res); // fallback
                continue; // next file
            }
    
            j->index = i; // remember the file’s sorted position
            queue_push(&q, j); // hand the job to workers
        }
    
        queue_close(&q); // no more jobs will be added
        queue_wait_all(&q); // wait until the queue is empty and workers are idle
    
        for (int i = 0; i < started; ++i) // join all started threads
            pthread_join(workers[i], NULL); // wait for worker i to finish
    }
    free(workers); // free the thread handle array

    size_t total_in = 0;  // total uncompressed bytes across all files
    size_t total_out = 0; // total compressed bytes across all files
    for (int i = 0; i < nfiles; ++i) { // write each file's result in lex order
        if (res.out_bufs[i] && res.out_sizes[i] > 0) { // only if we have compressed data
            uint32_t stored = (uint32_t)res.out_sizes[i]; // 4-byte length prefix (compressed size)
            fwrite(&stored, sizeof(uint32_t), 1, f_out);  // write size header
            fwrite(res.out_bufs[i], 1, res.out_sizes[i], f_out); // write compressed bytes
            total_in  += res.in_sizes[i];  // add original size
            total_out += res.out_sizes[i]; // add compressed size
        }
    }
    fclose(f_out); // finish the output file
    if (total_in > 0) { // avoid divide-by-zero if there were no bytes
        double saved = (double)(total_in - total_out) / (double)total_in; // fraction saved
        printf("Compression rate: %.2lf%%\n", saved * 100.0); // print percent saved
    }
    // cleanup section //
    for (int i = 0; i < nfiles; ++i) { // free per-file allocations
        free(files[i]); // free filename string
        free(res.out_bufs[i]); // free compressed buffer
    }
    free(files); // free filename list
    free(res.out_bufs); // and array of buffer pointers
    free(res.out_sizes);// and array of compressed sizes
    free(res.in_sizes); // lastly, free array of original sizes
}
    