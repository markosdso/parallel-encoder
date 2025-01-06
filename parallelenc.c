#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <fcntl.h>

#include <pthread.h>

#include <stdbool.h>

//Chunk sizes processed at a time
#define CHUNK_SIZE 4096

//Declare num_chunks as global variable so all functions can access
size_t num_chunks;

//Struct containing arguments for the functions called by the worker functions
typedef struct thread_args {
    unsigned char* chunk; 
    char* chunk_output;
    size_t chunk_size;
    size_t output_size;
    int chunk_idx;
    int complete_flag;
    pthread_mutex_t mutex;
} thread_args;

//Struct containing the worker thread details
typedef struct task_t {
    void (*function)(void*);
    void* arg;
} task_t;

//Struct containing the threadpool details
typedef struct threadpool_t {
    task_t* tasks;
    pthread_t* threads;
    int front;
    int back;
    int cur_size;
    int thread_count;
    bool stop;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} threadpool_t;

//Function containing each worker thread that will execute tasks in the task queue
void* worker_thread(void* arg) {

    //Cast argument to threadpool and store it in local variable
    threadpool_t* pool = (threadpool_t*)arg;

    //Continue processing chunks until all are processed. parallel stitching will handle the flag.
    while (1) {
        //Lock mutex while accessing/editing threadpool struct
        pthread_mutex_lock(&pool->mutex);

        //Wait and release mutex in the case that there are no threads to be processed
        while (pool->cur_size == 0 && pool->stop == false) {
            pthread_cond_wait(&pool->cond, &pool->mutex);
        }

        if (pool->stop) {
            pool->thread_count--;
            pthread_mutex_unlock(&pool->mutex);
            break;
        }

        //Edit threadpool struct to reflect task processed
        task_t task = pool->tasks[pool->front];
        pool->front = pool->front + 1;
        pool->cur_size--;

        //Exiting critical section
        pthread_mutex_unlock(&pool->mutex);

        //Call write_contents function for task
        task.function(task.arg);
  

    }
    //Return so no parallel execution is occuring during printing
    return NULL;

}

//Function to initialize the threadpool
threadpool_t* create_threadpool(int num_threads, int num_chunks) {

    //Initialize the threadpool and its struct variables
    threadpool_t* pool = (threadpool_t*)malloc(sizeof(threadpool_t));
    pool->tasks = (task_t*)malloc(sizeof(task_t) * num_chunks + num_threads + 1);
    pool->threads = (pthread_t*)malloc(sizeof(pthread_t) * num_threads);
    pool->front = 0;
    pool->back = 0;
    pool->cur_size = 0;
    pool->thread_count = num_threads;
    pool->stop = false;
    pthread_mutex_init(&pool->mutex, NULL);
    pthread_cond_init(&pool->cond, NULL);

    //Create the individual worker threads
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&pool->threads[i], NULL, worker_thread, pool);
        pthread_detach(pool->threads[i]);
    }

    //Return threadpool variable so it can be used in main function
    return pool;
}

//Function to add task to task queue
void add_task(threadpool_t* pool, void (*function)(void*), void* arg) {

    //Lock mutex while accessing/editing threadpool struct
    pthread_mutex_lock(&pool->mutex);

    //Edit threadpool struct to reflect task added
    pool->tasks[pool->back] = (task_t){ function, arg };
    pool->back = pool->back + 1;
    pool->cur_size++;

    //Signal that the task queue is not empty
    pthread_cond_signal(&pool->cond);

    //Exiting critical section
    pthread_mutex_unlock(&pool->mutex);
}

//Function to parse the argument line for files and # of worker threads
int parse_argv(int argc, char* argv[], int* num_threads) {

    /* Declare variables for the number of files to be concatenated and a flag to
    * show we have not reached I/O redirection, which are not processed as files.
    */ 
    int num_files = 0;
    int still_files = 1;

    //Code is sequential in the case that number of threads is not specified by -j
    *num_threads = 1;

    //Parse the command arguments
    for (int i = 1; i < argc && still_files; i++) {

        /* Handle -j to pull number of worker threads we should implement. Here, we 
        * remove -j jobs from the argument line so files can be processed/concatanated
        * uninhibited in the main function
        */
        if (strcmp(argv[i], "-j") == 0) {
            if (i + 1 < argc) {
                //Grab number ahead of -j
                *num_threads = atoi(argv[i + 1]);

                //Only values greater than 0 are valid
                if (*num_threads <= 0) {
                    fprintf(stderr, "Error: invalid number of threads specified after -j.\n");
                    exit(1);
                }

                //Shift command arguments to remove -j jobs
                for (int j = i; j < argc - 2; j++) {
                    argv[j] = argv[j + 2];
                }

                //Reduce argc to reflect shift, remain at the same place in argv since -j jobs was removed
                argc -= 2; 
                i--;       
            }
            //Handle error if no integer arg specified
            else {
                fprintf(stderr, "Error: -j option requires an integer argument.\n");
                exit(1);
            }
        }
        //End parsing when I/O redirection is reached
        else if ((strcmp(argv[i], ">") == 0) || (strcmp(argv[i], ">>") == 0) || (strcmp(argv[i], "<") == 0) || (strcmp(argv[i], "|") == 0)) {
            still_files = 0;
        }
        //Increment number of files in every other case
        else {
            num_files++;
        }
    }

    //Return number of files
    return num_files;
}

//Function that maps a file's contents to the maps array in main, to be concatenated later
char* map_file(const char* filename, size_t* file_size) {

    //Open file descriptor 
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "Error: file does not exist.\n");
        return NULL;
    }

    //Access sb to retrieve file size
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        fprintf(stderr, "Error: an I/O error occurred while reading from the file system.\n");
        close(fd);
        return NULL;
    }
    *file_size = sb.st_size;

    //Map file to an address in memory
    char* addr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

    //Close file descriptor and handle the case that mapping has failed
    close(fd);
    if (addr == MAP_FAILED) {
        fprintf(stderr, "Error: mapping file to memory address failed.\n");
        return NULL;
    }

    //Return pointer to mapped file
    return addr;
}

//Function responsible for the encoding of each chunk in the chunks_array
void write_contents(void* chunk_struct) {

    //Cast argument to thread_args and store it in local variable
    thread_args* args = (thread_args*)chunk_struct;

    pthread_mutex_lock(&args->mutex);

    //Initialize the variables that will keep track of our parsing and encoding of the chunk
    unsigned char* cur = args->chunk;
    unsigned char prev = '\0';
    int num = 0;
    unsigned char binary_num;
    char* ptr = args->chunk_output;

    //Parse the full chunk
    for (size_t i = 0; i < args->chunk_size; i++) {
        //Increase count if character is repeated
        if (i == 0 || *cur == prev) {
            num++;
        }
        //Encode the group of repeated characters
        else {
            binary_num = num & 0xFF;
            *ptr++ = prev;
            *ptr++ = binary_num;
            num = 1;
            args->output_size += 2;
        }

        //Update parsing variables
        prev = *cur;
        cur++;
    }

    //Handle final group of repeated characters 
    binary_num = num & 0xFF;
    *ptr++ = prev;
    *ptr++ = binary_num;
    args->output_size += 2;

    //Cap with null terminator
    *ptr = '\0'; 

    //Signal that this chunk has been encoded
    args->complete_flag = 1;

    pthread_mutex_unlock(&args->mutex);
}

//Function which stitches encoded threads in parallel with worker_thread function
void parallel_thread_stitching(thread_args** chunks_array) {

    //Initialize the variables keeping track of the chunk which we stitch to
    char prev_char = '\0';
    int prev_count = 0;
    size_t prev_chunk_idx;

    //Parse all chunks 
    for (size_t chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
        pthread_mutex_lock(&chunks_array[chunk_idx]->mutex);

        //Only stitch chunk if it has been encoded
        if (chunks_array[chunk_idx]->complete_flag) {

            //Store current chunk and chunk we stitch to in discrete variables to make code more organized
            char* chunk_output = chunks_array[chunk_idx]->chunk_output;
            char* prev_chunk_output = chunks_array[prev_chunk_idx]->chunk_output;

            /* Stitch in the case that the beginning of one chunk encodes the same character as the end of 
            * the previous NON-EMPTY chunk. In the case that stitching occurs over greater than 2 chunks,
            * we may have to go back multiple chunks, and that can be done by skipping over empty chunks.
            */
            if (chunk_idx != 0 && prev_char == chunk_output[0]) {

                //Increment count of the last encoded character of the previous non-empty chunk
                int current_count = chunk_output[1];
                prev_count += current_count;

                //Replace value of the last encoded character of prev non-empty chunk and reduce cur chunk
                prev_chunk_output[chunks_array[prev_chunk_idx]->output_size - 1] = prev_count & 0xFF;
                memmove(chunk_output, chunk_output + 2, chunks_array[chunk_idx]->output_size - 1);
                chunks_array[chunk_idx]->output_size -= 2;
            }

            //Move prev chunk forward in the case that unique characters remain in the cur chunk
            if (chunks_array[chunk_idx]->output_size > 0) {
                prev_char = chunk_output[chunks_array[chunk_idx]->output_size - 2];
                prev_count = chunk_output[chunks_array[chunk_idx]->output_size - 1];
                prev_chunk_idx = chunk_idx;
            }
            pthread_mutex_unlock(&chunks_array[chunk_idx]->mutex);

        }
        //Continue and retry same chunk index in the case that it has not yet been encoded
        else {
            pthread_mutex_unlock(&chunks_array[chunk_idx]->mutex);
            chunk_idx--;
        }
    }
    
}

//Main function! Finally!
int main(int argc, char* argv[]) {

    //Extract the number of files, number of jobs specified, and remove -j jobs command from argument line
    int num_threads;
    int num_files = parse_argv(argc, argv, &num_threads);

    //Exit if no files specified
    if (num_files < 1) {
        fprintf(stderr, "Error: no files provided as arguments.\n");
        exit(1);
    }

    //Initialize variables to handle mapping and concatenation of file contents
    size_t total_size = 0;
    size_t* file_sizes = malloc(num_files * sizeof(size_t));
    char** maps = malloc(num_files * sizeof(char*));

    //Map file contents to array and gather the total size of all files combined
    for (int i = 1; i <= num_files; i++) {
        maps[i - 1] = map_file(argv[i], &file_sizes[i - 1]);
        if (maps[i - 1] == NULL) {
            fprintf(stderr, "Error: map failed\n");
            exit(1);
        }
        total_size += file_sizes[i - 1];
    }

    //Create array for full contents and handle the case the allocation fails
    char* contents = malloc(total_size + 1);
    if (!contents) {
        fprintf(stderr, "Error: memory allocation failed\n");
        exit(1);
    }

    //Copy the contents of maps to contents in order to concatenate all file contents
    char* map = contents;
    for (int i = 0; i < num_files; i++) {
        memcpy(map, maps[i], file_sizes[i]);
        map += file_sizes[i];
    }

    free(file_sizes);
    free(maps);

    //Gather number of chunks and assign to global variable
    num_chunks = (total_size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    //Initialize the chunk array containing the thread arguments and outputs
    thread_args** chunks_array = malloc(num_chunks * sizeof(thread_args));

    //Initialize the thread pool
    threadpool_t* pool = create_threadpool(num_threads, num_chunks);
    
    //Create a unique task for every individual chunk and add each to the task queue
    for (size_t offset = 0, chunk_idx = 0; offset < total_size; offset += CHUNK_SIZE, chunk_idx++) {

        //Initialize chunk_size variable and handle edge case of last chunk
        size_t chunk_size;
        if (offset + CHUNK_SIZE > total_size) {
            chunk_size = total_size - offset;
        }
        else {
            chunk_size = CHUNK_SIZE;
        }

        //Allocate memory for the task arguments of unique chunk and initialize its struct variables
        chunks_array[chunk_idx] = malloc(sizeof(thread_args));
        chunks_array[chunk_idx]->chunk = (unsigned char*)(contents + offset);
        chunks_array[chunk_idx]->chunk_output = malloc(CHUNK_SIZE * 2 + 1);
        chunks_array[chunk_idx]->chunk_size = chunk_size;
        chunks_array[chunk_idx]->chunk_idx = chunk_idx;
        chunks_array[chunk_idx]->complete_flag = 0;
        chunks_array[chunk_idx]->output_size = 0;
        pthread_mutex_init(&chunks_array[chunk_idx]->mutex, NULL);

        //Add task to queue for encoding unique chunk
        add_task(pool, (void (*)(void*))write_contents, chunks_array[chunk_idx]);

    }

    //Stitch chunk_outputs in parallel with function of worker threads
    parallel_thread_stitching(chunks_array);

    pthread_mutex_lock(&pool->mutex);
    pool->stop = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    //Print the fully processed (encoded and stitched) chunk_outputs
    for (size_t chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
        char* chunk_output = chunks_array[chunk_idx]->chunk_output;
        write(STDOUT_FILENO, chunk_output, chunks_array[chunk_idx]->output_size);
        pthread_mutex_destroy(&chunks_array[chunk_idx]->mutex);
        free(chunks_array[chunk_idx]->chunk_output);
        free(chunks_array[chunk_idx]);
    }

    while(1) {
        pthread_mutex_lock(&pool->mutex);
        fflush(stdout);
        if (pool->thread_count == 0) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }
        pthread_mutex_unlock(&pool->mutex);
    }

    // Destroy synchronization primitives
    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->cond);

    // Free allocated resources
    free(pool->tasks);
    free(pool->threads);
    free(pool);

}
