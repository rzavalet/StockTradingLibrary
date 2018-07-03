/*
 * =====================================================================================
 *
 *       Filename:  test3.c
 *
 *    Description:  Show that we can refresh stock values
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

int test()
{
  int i;
  BENCHMARK_H   benchmarkH = NULL;

  /* Perform an initial load */
  fprintf(stdout, "Performing initial load\n");
  if (benchmark_initial_load("MyTest",
                             CHRONOS_SERVER_HOME_DIR,
                             CHRONOS_SERVER_DATAFILES_DIR) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to perform initial load\n");
    goto failXit;
  }

  fprintf(stdout, "\n");
  fprintf(stdout, "Allocating benchmark handle\n");
  if (benchmark_handle_alloc(&benchmarkH, 0, "MyTest1", 
                             CHRONOS_SERVER_HOME_DIR, 
                             CHRONOS_SERVER_DATAFILES_DIR) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to allocate benchmark handle\n");
    goto failXit;
  }

  for (i = 0; i < 100; i++) {
    int rc = SUCCESS;
    fprintf(stdout, "\n");
    fprintf(stdout, "Retrieving stocks info for symbol: %d\n", i);
    rc = benchmark_view_stock(benchmarkH, &i);
    if (rc != SUCCESS) {
      fprintf(stderr, "ERROR: Failed to retrieve info for symbol: %d\n", i);
      goto failXit;
    }
  }

  fprintf(stdout, "\n");
  fprintf(stdout, "Freeing benchmark handle\n");
  if (benchmark_handle_free(benchmarkH) != SUCCESS) {
    fprintf(stderr, "ERROR: Failed to free benchmark handle\n");
    goto failXit;
  }
  benchmarkH = NULL;

  fprintf(stdout, "\n");
  fprintf(stdout, "++ Test PASSED\n");
  return SUCCESS;

failXit:
  fprintf(stdout, "\n");
  fprintf(stdout, "++ Test FAILED\n");

  if (benchmarkH) {
    benchmark_handle_free(benchmarkH);
    benchmarkH = NULL;
  }

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
