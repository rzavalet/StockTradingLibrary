/*
 * data_packet.c
 *
 *  Created on: Jan 20, 2018
 *      Author: Ricardo Zavaleta
 */

#include "benchmark_common.h"

void *
benchmark_data_packet_alloc(size_t reqsz)
{
  benchmark_data_packet_t *packetP = NULL;

  if (reqsz <= 0) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  packetP = malloc(sizeof(benchmark_data_packet_t));
  if (packetP == NULL) {
    benchmark_error("Could not allocate packet structure");
    goto failXit;
  }

  memset(packetP, 0, sizeof(benchmark_data_packet_t));

  packetP->data = calloc(reqsz, sizeof(benchmark_xact_data_t));
  if (packetP->data == NULL) {
    benchmark_error("Could not allocate data structure");
    goto failXit;
  }

  packetP->size = reqsz;
  packetP->used = 0;

  return packetP;

failXit:

  if (packetP != NULL) {
    if (packetP->data != NULL) {
      free(packetP->data);
      packetP->data = NULL;
    }

    free(packetP);
    packetP = NULL;
  }

  return NULL;
}

int
benchmark_data_packet_free(void *data_packetH)
{
  benchmark_data_packet_t *packetP = NULL;

  if (data_packetH == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  packetP = data_packetH;

  if (packetP->data != NULL) {
    free(packetP->data);
    packetP->data = NULL;
  }

  free(packetP);

  return BENCHMARK_SUCCESS;

  failXit:
    return BENCHMARK_FAIL;
}

int
benchmark_data_packet_append(const char  *accountId,
                             int          symbolId,
                             const char  *symbol,
                             float        price,
                             int          amount,
                             void        *data_packetH)
{
  benchmark_data_packet_t *packetP = NULL;
  int idx;

  if (data_packetH == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  packetP = data_packetH;

  assert(packetP->used <= packetP->size);

  if (packetP->used >= packetP->size) {
    benchmark_error("packet is full");
    goto failXit;
  }

  idx = packetP->used;
  strncpy(packetP->data[idx].accountId, accountId, sizeof(packetP->data[idx].accountId));
  packetP->data[idx].symbolId = symbolId;
  strncpy(packetP->data[idx].symbol, symbol, sizeof(packetP->data[idx].symbol));
  packetP->data[idx].price = price;
  packetP->data[idx].amount = amount;

  packetP->used ++;

  return BENCHMARK_SUCCESS;

  failXit:
    return BENCHMARK_FAIL;
}
