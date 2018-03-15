/*
 * =====================================================================================
 *
 *       Filename:  sell_txn.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/16/16 23:33:23
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ricardo Zavaleta (), rj.zavaleta@gmail.com
 *   Organization:  Cinvestav
 *
 * =====================================================================================
 */

#include "common/benchmark_common.h"

int
benchmark_sell(int account, int symbol, float price, int amount, int force_apply, void *benchmark_handle, int *symbol_ret)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  int ret;
  int symbol_idx;
  char *random_symbol;
  float random_price;
  int random_amount;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  if (symbol < 0) {
    symbol_idx = rand() % benchmarkP->number_stocks;
    random_symbol = benchmarkP->stocks[symbol_idx];
  }
  else {
    random_symbol = benchmarkP->stocks[symbol];
  }

  if (price < 0) {
    random_price = rand() % 100 + 1;
  }
  else {
    random_price = price;
  }

  if (amount < 0) {
    random_amount = rand() % 20 + 1;
  }
  else {
    random_amount = amount;
  }
 
  assert("Need to pass a valid account" == NULL);
  ret = sell_stocks(NULL, random_symbol, random_price, random_amount, force_apply, NULL, benchmarkP);
  if (ret != 0) {
    benchmark_error("Could not place order");
    goto failXit;
  }

  if (symbol_ret != NULL) {
    *symbol_ret = symbol;
  }
      
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return ret;
  
 failXit:
  if (symbol_ret != NULL) {
    *symbol_ret = -1;
  }
      
  return BENCHMARK_FAIL;
}

int
benchmark_sell2(void *data_packetH,
                void *benchmark_handle)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  benchmark_data_packet_t *packetP = NULL;
  benchmark_xact_h xactH = NULL;
  int i;
  int ret;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL || data_packetH == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  packetP = data_packetH;

  ret = start_xact(&xactH, "SELL_TXN", benchmarkP);
  if (ret != BENCHMARK_SUCCESS) {
    goto failXit;
  }

  benchmark_debug(2, "Sell for: %d symbols", packetP->used);

  for (i=0; i<packetP->used; i++) {
    benchmark_debug(2, "Placing order for user: %s", packetP->data[i].accountId);
    ret = sell_stocks(packetP->data[i].accountId,
                      packetP->data[i].symbol,
                      packetP->data[i].price,
                      packetP->data[i].amount,
                      1, xactH, benchmarkP);
    if (ret != BENCHMARK_SUCCESS) {
      benchmark_error("Could not place order for user: %s and symbol: %s", packetP->data[i].accountId, packetP->data[i].symbol);
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
