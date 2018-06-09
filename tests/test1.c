/*
 * =====================================================================================
 *
 *       Filename:  test1.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/03/2018 03:49:47 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  RICARDO ZAVALETA (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#include <stdio.h>
#include "benchmark.h"

#define CHRONOS_SERVER_HOME_DIR       "/tmp/chronos/databases"
#define CHRONOS_SERVER_DATAFILES_DIR  "/tmp/chronos/datafiles"
#define SUCCESS 0
#define FAIL    1

/* 
 * Test 1: Make sure we can allocate a benchmark handle and then release
 * */
int test()
{
  BENCHMARK_H   benchmarkH = NULL;

  fprintf(stdout, "Allocating benchmark handle\n");
  if (benchmark_handle_alloc(&benchmarkH, 1, "MyTest1", 
                             CHRONOS_SERVER_HOME_DIR, 
                             CHRONOS_SERVER_DATAFILES_DIR) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to allocate benchmark handle\n");
    goto failXit;
  }

  fprintf(stdout, "Freeing benchmark handle\n");
  if (benchmark_handle_free(benchmarkH) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to free benchmark handle\n");
    goto failXit;
  }

  benchmarkH = NULL;

  /* Trying to create databases when already created */
  fprintf(stdout, "\n");
  fprintf(stdout, "Allocating benchmark handle\n");
  if (benchmark_handle_alloc(&benchmarkH, 1, "MyTest1", 
                             CHRONOS_SERVER_HOME_DIR, 
                             CHRONOS_SERVER_DATAFILES_DIR) == SUCCESS) {
    fprintf(stderr, "ERROR: Shouldn't be able to create dbs if previously created\n");
    goto failXit;
  }

  benchmarkH = NULL;

  fprintf(stdout, "\n");
  fprintf(stdout, "Allocating benchmark handle\n");
  if (benchmark_handle_alloc(&benchmarkH, 0, "MyTest1", 
                             CHRONOS_SERVER_HOME_DIR, 
                             CHRONOS_SERVER_DATAFILES_DIR) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to allocate benchmark handle\n");
    goto failXit;
  }

  fprintf(stdout, "\n");
  fprintf(stdout, "Freeing benchmark handle\n");
  if (benchmark_handle_free(benchmarkH) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to free benchmark handle\n");
    goto failXit;
  }

  fprintf(stdout, "\n");
  fprintf(stdout, "++ Test PASSED\n");
  return SUCCESS;

failXit:
  fprintf(stdout, "\n");
  fprintf(stdout, "++ Test FAILED\n");
  return FAIL;
}

int main()
{
  if (test() != SUCCESS) {
    fprintf(stderr, "ERROR: Failure in test");
    goto failXit;
  }

  return SUCCESS;

failXit:
  return FAIL;
}

