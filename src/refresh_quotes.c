/*
 * =====================================================================================
 *
 *       Filename:  benchmark_initial_load.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/14/16 19:31:17
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ricardo Zavaleta (rj.zavaleta@gmail.com)
 *   Organization:  Cinvestav
 *
 * =====================================================================================
 */

#include "common/benchmark_common.h"

int
benchmark_refresh_quotes(void *benchmark_handle, int *symbolP, float newValue)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  int symbol;
  char *random_symbol;
  int ret = BENCHMARK_SUCCESS;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  if (symbolP != NULL && *symbolP >= 0) {
    symbol = *symbolP;
  }
  else {
    symbol = rand() % benchmarkP->number_stocks;
  }
  random_symbol = benchmarkP->stocks[symbol];

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_API, "PID: %d, Attempting to update %s to %f", getpid(), random_symbol, newValue);
  ret = update_stock(random_symbol, newValue, NULL, benchmarkP);
  if (ret != 0) {
    benchmark_error("Could not update quote");
    goto failXit;
  }

  if (symbolP != NULL) {
    *symbolP = symbol;
  }
    
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_API, "Done refreshing price for %s.", random_symbol);
  return ret;
  
 failXit:
  if (symbolP != NULL) {
    *symbolP = -1;
  }
  
  return BENCHMARK_FAIL;
}

int
benchmark_refresh_quotes2(void *benchmark_handle, const char *symbolP, float newValue)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  int ret = BENCHMARK_SUCCESS;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL) {
    goto failXit;
  }

  if (symbolP == NULL || symbolP[0] == '\0') {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_API,"PID: %d, Attempting to update %s to %f", getpid(), symbolP, newValue);
  ret = update_stock((char *)symbolP, newValue, NULL, benchmarkP);
  if (ret != 0) {
    benchmark_error("Could not update quote");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_API, "Done refreshing price for %s.", symbolP);
  return ret;
  
 failXit:
  
  return BENCHMARK_FAIL;
}

/*------------------------------------------------------------
 * Updates the quotes of the provided symbols with the
 * provided prices. All the updates happen in the 
 * same transaction.
 *----------------------------------------------------------*/
int
benchmark_refresh_quotes_list(int           num_symbols,
                              const char  **symbols_list,
                              float        *prices_list,
                              void         *benchmark_handle)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  benchmark_xact_h xactH = NULL;
  int i;
  int ret = BENCHMARK_SUCCESS;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  if (symbols_list == NULL || prices_list == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  ret = start_xact(&xactH, "REFRESH_STOCK_TXN", benchmarkP);
  if (ret != BENCHMARK_SUCCESS) {
    goto failXit;
  }

  for (i=0; i<num_symbols; i++) {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_API,
                    "PID: %d, Attempting to update %s to %f", 
                    getpid(), symbols_list[i], prices_list[i]);

    ret = update_stock((char *)symbols_list[i], prices_list[i], xactH, benchmarkP);
    if (ret != BENCHMARK_SUCCESS) {
      benchmark_error("Could not update quote");
      goto failXit;
    }
  }

  ret = commit_xact(xactH, benchmarkP);
  if (ret != BENCHMARK_SUCCESS) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_API, 
                  "Done refreshing price for %d symbols.", num_symbols);
  return ret;
  
 failXit:
  if (xactH != NULL) {
    abort_xact(xactH, benchmarkP);
  }
  
  return BENCHMARK_FAIL;
}
