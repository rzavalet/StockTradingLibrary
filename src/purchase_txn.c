/*
 * =====================================================================================
 *
 *       Filename:  purchace_txn.c
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
benchmark_purchase(int account, 
                   int symbol, 
                   float price, 
                   int amount, 
                   int force_apply, 
                   void *benchmark_handle, 
                   int *symbolP)
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
 
  assert("Need to set account id" == NULL);
  ret = place_order(NULL, random_symbol, random_price, random_amount, force_apply, NULL, benchmarkP);
  if (ret != 0) {
    fprintf(stderr, "Could not place order\n");
    goto failXit;
  }

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
benchmark_purchase2(void  *data_packetH,
                    void  *benchmark_handle)
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

  ret = start_xact(&xactH, "PURCHASE_TXN", benchmarkP);
  if (ret != BENCHMARK_SUCCESS) {
    goto failXit;
  }

  benchmark_debug(2, "Purchasing for: %d symbols", packetP->used);

  for (i=0; i<packetP->used; i++) {
    benchmark_debug(2, "Placing order for user: %s", packetP->data[i].accountId);
    ret = place_order(packetP->data[i].accountId,
                      packetP->data[i].symbol,
                      packetP->data[i].price,
                      packetP->data[i].amount,
                      1, xactH, benchmarkP);
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
