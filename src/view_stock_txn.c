/*
 * =====================================================================================
 *
 *       Filename:  view_stock_txn.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/16/16 23:33:23
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ricardo Zavaleta (), rj.zavaleta@gmail.com
 *   Organization:  
 *
 * =====================================================================================
 */

#include "common/benchmark_common.h"
#include "benchmark_stocks.h"



int
benchmark_view_stock(void *benchmark_handle, int *symbolP)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  int ret;
  int symbol;
  char *random_symbol;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL) {
    benchmark_error("Invalid benchmark handle");
    goto failXit;
  }
  
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  if (benchmarkP->stocks == NULL || benchmarkP->number_stocks <= 0) {
    benchmark_error("Stock list has not been loaded");
    goto failXit;
  }

  if (symbolP != NULL && *symbolP >= 0) {
    symbol = *symbolP;
  }
  else {
    symbol = rand() % benchmarkP->number_stocks;
  } 

  random_symbol = benchmarkP->stocks[symbol];
#if 0
  ret = show_stocks_records(random_symbol, benchmarkP);
#endif
  ret = show_quote(random_symbol, NULL, benchmarkP);

  if (symbolP != NULL) {
    *symbolP = symbol;
  }
    
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  return ret;

 failXit:
  if (symbolP != NULL) {
    *symbolP = -1;
  }  
  
  return BENCHMARK_FAIL;
}

int
benchmark_view_stock2(int num_symbols, const char **symbol_list_P, void *benchmark_handle)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  benchmark_xact_h xactH = NULL;
  int i;
  int ret;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL) {
    benchmark_error("Invalid benchmark handle");
    goto failXit;
  }
  
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  ret = start_xact(&xactH, "VIEW_STOCK_TXN", benchmarkP);
  if (ret != BENCHMARK_SUCCESS) {
    goto failXit;
  }

  benchmark_debug(2, "Showing quotes for: %d symbols", num_symbols);

  for (i=0; i<num_symbols; i++) {
    benchmark_debug(2, "Showing quote for symbol: %s", symbol_list_P[i]);
    ret = show_quote((char *)symbol_list_P[i], xactH, benchmarkP);
    if (ret != BENCHMARK_SUCCESS) {
      goto failXit;
    }
  }

  ret = commit_xact(xactH, benchmarkP);
  if (ret != BENCHMARK_SUCCESS) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  return ret;

 failXit:
  if (xactH != NULL) {
    abort_xact(xactH, benchmarkP);
  }

  return BENCHMARK_FAIL;
}
