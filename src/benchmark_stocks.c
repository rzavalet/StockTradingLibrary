#include "common/benchmark_common.h"
#include "benchmark_stocks.h"

int
benchmark_stocks_symbols_print(BENCHMARK_DBS *benchmarkP)
{
  int      rc = BENCHMARK_SUCCESS;

  if (benchmarkP==NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  if (benchmarkP->number_stocks > 0 && benchmarkP->stocks != NULL) {
    int i;
    for (i=0; i<benchmarkP->number_stocks; i++) {
      if (benchmarkP->stocks[i] != NULL) {
        benchmark_info("%d\t%s", i, benchmarkP->stocks[i]);
      }
    }
  }

  goto cleanup;

failXit:
  rc = BENCHMARK_FAIL;

cleanup:
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  return rc;
}


/*-------------------------------------------------------
 * Iterates over the stored stocks and obtains the 
 * names. The names are stored in a list of strings
 * in the BENCHMARK_DBS structure.
 *-----------------------------------------------------*/
int
benchmark_stocks_symbols_get(BENCHMARK_DBS *benchmarkP)
{
  DBC     *cursorP = NULL;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  DB_BTREE_STAT  *stocks_statsP = NULL;
  DBT      key, data;
  int      ret;
  int      rc = BENCHMARK_SUCCESS;
  int      current_slot = 0;

  if (benchmarkP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  benchmark_debug(5, "PID: %d, Allocating space for %d stocks", 
                  getpid(), benchmarkP->number_stocks);

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  ret = envP->txn_begin(envP, 
                        NULL, 
                        &txnP, 
                        DB_READ_COMMITTED | DB_TXN_WAIT);
  if (ret != 0) {
    envP->err(envP, ret, 
              "[%s:%d] [%d] Transaction begin failed.", 
              __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* Get number of stocks in the database */
  rc = benchmarkP->stocks_dbp->stat(benchmarkP->stocks_dbp, 
                                    txnP, 
                                    (void *)&stocks_statsP, 
                                    0 /* no FAST_STAT */);
  if (rc != 0) {
    envP->err(envP, rc, 
              "[%s:%d] [%d] Failed to obtain stats from Stocks table.", 
              __FILE__, __LINE__, getpid());
    goto failXit; 
  }
  
  benchmarkP->number_stocks = stocks_statsP->bt_nkeys;
  benchmark_debug(5, 
                  "Number of keys in Stocks table is: %d", 
                  benchmarkP->number_stocks);

  /* Allocate an array of chars to hold the info 
   * of the stocks */
  benchmarkP->stocks = calloc(benchmarkP->number_stocks, sizeof (char *));
  if (benchmarkP->stocks == NULL) {
    benchmark_error("Could not allocate storage for stocks list");
    goto failXit;
  }

  ret = benchmarkP->stocks_dbp->cursor(benchmarkP->stocks_dbp, 
                                       txnP,
                                       &cursorP, 
                                       DB_READ_COMMITTED);
  if (ret != 0) {
    envP->err(envP, ret, 
              "[%s:%d] [%d] Failed to create cursor for Stocks.", 
              __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* Iterate over the database, retrieving each record in turn. */
  while ((ret = cursorP->get(cursorP, 
                             &key, 
                             &data, 
                             DB_NEXT | DB_READ_COMMITTED)) == 0) 
  {
    benchmark_debug(5, "PID: %d, Copying stock number %d, %s", 
                    getpid(), current_slot, (char *)key.data);

    assert(0 <= current_slot && current_slot < benchmarkP->number_stocks);

    /* Copy the key in the list */
    benchmarkP->stocks[current_slot] = strdup((char *)key.data);
    current_slot ++;
  }

  if (ret != DB_NOTFOUND) {
    envP->err(envP, rc, 
              "[%s:%d] [%d] Failed to find record in Quotes.", 
              __FILE__, __LINE__, getpid());
    goto failXit;
  }

  ret = cursorP->close(cursorP);
  if (ret != 0) {
    envP->err(envP, ret, 
              "[%s:%d] [%d] Failed to close cursor.", 
              __FILE__, __LINE__, getpid());
  }
  cursorP = NULL;

  benchmark_info("PID: %d, Committing transaction: %p", 
                  getpid(), txnP);
  ret = txnP->commit(txnP, 0);
  if (ret != 0) {
    envP->err(envP, rc, 
              "[%s:%d] [%d] Transaction commit failed. txnP: %p", 
              __FILE__, __LINE__, getpid(), txnP);
    goto failXit; 
  }
  txnP = NULL;

  goto cleanup;

failXit:
  rc = BENCHMARK_FAIL;

  if (txnP != NULL) {
    if (cursorP != NULL) {
      ret = cursorP->close(cursorP);
      if (ret != 0) {
        envP->err(envP, ret, 
                  "[%s:%d] [%d] Failed to close cursor.", 
                  __FILE__, __LINE__, getpid());
      }
      cursorP = NULL;
    }

    benchmark_info("PID: %d About to abort transaction. txnP: %p", 
                    getpid(), txnP);
    ret = txnP->abort(txnP);
    if (ret != 0) {
      envP->err(envP, ret, 
                "[%s:%d] [%d] Transaction abort failed.", 
                __FILE__, __LINE__, getpid());
    }
  }

  if (benchmarkP->number_stocks > 0 && benchmarkP->stocks != NULL) {
    int i;
    for (i=0; i<benchmarkP->number_stocks; i++) {
      if (benchmarkP->stocks[i] != NULL) {
        free(benchmarkP->stocks[i]);
        benchmarkP->stocks[i] = NULL;
      }
    }
  }

cleanup:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return rc;
}

int
benchmark_stocks_stats_get(BENCHMARK_DBS *benchmarkP)
{
  int             rc = BENCHMARK_SUCCESS;
  DB             *quotes_dbp = NULL;
  DB_BTREE_STAT  *quotes_statsP = NULL;
  DB_ENV         *envP = NULL;

  if (benchmarkP == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }
  quotes_dbp = benchmarkP->quotes_dbp;
  if (quotes_dbp == NULL) {
    benchmark_error("Quotes table is uninitialized");
    goto failXit;
  }

  rc = quotes_dbp->stat(quotes_dbp, NULL, (void *)&quotes_statsP, 0 /* no FAST_STAT */);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to obtain stats from Quotes table.", __FILE__, __LINE__, getpid());
    goto failXit; 
  }

  benchmarkP->number_stocks = quotes_statsP->bt_nkeys;
  benchmark_debug(5, "Number of keys in Quotes table is: %d", benchmarkP->number_stocks);

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  rc = BENCHMARK_FAIL;

cleanup:
  if (quotes_statsP) {
    free(quotes_statsP);
  }

  return rc;
}


/*-------------------------------------------------------
 * Gets the list of stocks from the benchmark handle.
 *-----------------------------------------------------*/
int
benchmark_stock_list_get(void   *benchmark_handle, 
                         char ***stocks_list, 
                         int    *num_stocks)
{
  int            rc = BENCHMARK_SUCCESS;
  BENCHMARK_DBS *benchmarkP = NULL;

  benchmarkP = benchmark_handle;
  if (benchmarkP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (benchmarkP->number_stocks == 0 || benchmarkP->stocks == NULL) {
    rc = benchmark_stocks_symbols_get(benchmarkP);
    if (rc != BENCHMARK_SUCCESS) {
      goto failXit;
    }
  }

  *stocks_list = benchmarkP->stocks; 
  *num_stocks = benchmarkP->number_stocks;
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  goto cleanup;
  
failXit:
  rc = BENCHMARK_FAIL;

cleanup:
  return rc;
}

/*-------------------------------------------------------
 * Gets the list of stocks from the stocks files that
 * is used initially to populate the stocks database.
 *-----------------------------------------------------*/
int
benchmark_stock_list_from_file_get(const char      *homedir,
                                   const char      *datafilesdir,
                                   unsigned int     num_stocks,
                                   char          ***stock_list_ret)
{
  int rc = BENCHMARK_SUCCESS;
  int current_slot = 0;
  int size;
  char *stocks_file = NULL;
  char **listP = NULL;
  FILE *ifp;
  char buf[MAXLINE];
  char ignore_buf[500];
  STOCK my_stocks;

  assert(homedir != NULL && homedir[0] != '\0');
  assert(datafilesdir != NULL && datafilesdir[0] != '\0');

  if (stock_list_ret == NULL || num_stocks <= 0) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  size = strlen(datafilesdir) + strlen(STOCKS_FILE) + 2;
  stocks_file = malloc(size);
  if (stocks_file == NULL) {
    benchmark_error("Failed to allocate memory.");
    goto failXit;
  }
  snprintf(stocks_file, size, "%s/%s", datafilesdir, STOCKS_FILE);

  listP = calloc(num_stocks, sizeof (char *));
  if (listP == NULL) {
    benchmark_error("Could not allocate storage for stocks list");
    goto failXit;
  }

  ifp = fopen(stocks_file, "r");
  if (ifp == NULL) {
    benchmark_error("Error opening file '%s'", stocks_file);
    goto failXit;
  }

  /* Iterate over the vendor file */
  while (fgets(buf, MAXLINE, ifp) != NULL
          && current_slot < num_stocks) {

    /* zero out the structure */
    memset(&my_stocks, 0, sizeof(STOCK));

    /*
     * Scan the line into the structure.
     * Convenient, but not particularly safe.
     * In a real program, there would be a lot more
     * defensive code here.
     */
    sscanf(buf,
      "%10[^#]#%128[^#]#%500[^\n]",
      my_stocks.stock_symbol, my_stocks.full_name, ignore_buf);

    listP[current_slot] = strdup(my_stocks.stock_symbol);
    current_slot ++;
  }

  fclose(ifp);
  goto cleanup;

failXit:
  if (ifp != NULL) {
    fclose(ifp);
  }

  if (listP != NULL) {
    int i;
    for (i=0; i<num_stocks; i++) {
      if (listP[i] != NULL) {
        free(listP[i]);
        listP[i] = NULL;
      }
    }

    free(listP);
    listP = NULL;
  }

  rc = BENCHMARK_FAIL;

cleanup:
  if (stock_list_ret != NULL) {
    *stock_list_ret = listP;
  }

  return rc;
}


