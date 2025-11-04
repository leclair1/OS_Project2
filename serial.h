// serial.h
#ifndef SERIAL_H
#define SERIAL_H
// here we expose a single entry we will call from main
// we let caller decide thread count
int run_parallel(const char *input_dir, const char *output_zip, int requested_threads);
#endif
