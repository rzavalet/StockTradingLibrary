/*
 * =====================================================================================
 *
 *       Filename:  test2.c
 *
 *    Description:  Show how to perform an initial load 
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
 * Test for initial load
 * */
int test()
{
  BENCHMARK_H   benchmarkH = NULL;

  /* Perform an initial load */
  fprintf(stdout, "Performing initial load\n");
  if (benchmark_initial_load("MyTest",
                             CHRONOS_SERVER_HOME_DIR,
                             CHRONOS_SERVER_DATAFILES_DIR) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to perform initial load\n");
    goto failXit;
  }

  /* One databases are created, we should not be able to create them again */
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

