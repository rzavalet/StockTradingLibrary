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

/*============================================================================
 *                          PROTOTYPES
 *============================================================================*/
static int
load_currencies_database(BENCHMARK_DBS *benchmarkP, const char *currencies_file);

static int
load_stocks_database(BENCHMARK_DBS *benchmarkP, const char *stocks_file);

static int
load_personal_database(BENCHMARK_DBS *benchmarkP, const char *personal_file);

static int
load_quotes_database(BENCHMARK_DBS *benchmarkP, const char *quotes_file);

int 
benchmark_initial_load(const char *program,
                       const char *homedir, 
                       const char *datafilesdir) 
{
  void *benchmarkP = NULL;
  char *personal_file = NULL;
  char *stocks_file = NULL;
  char *currencies_file = NULL;
  char *quotes_file = NULL;
  int size;
  int ret;

  assert(homedir != NULL && homedir[0] != '\0');
  assert(datafilesdir != NULL && datafilesdir[0] != '\0');
  
  /* Find our input files */
  size = strlen(datafilesdir) + strlen(PERSONAL_FILE) + 2;
  personal_file = malloc(size);
  if (personal_file == NULL) {
    benchmark_error("Failed to allocate memory.");
    goto failXit;
  }
  snprintf(personal_file, size, "%s/%s", datafilesdir, PERSONAL_FILE);

  size = strlen(datafilesdir) + strlen(STOCKS_FILE) + 2;
  stocks_file = malloc(size);
  if (stocks_file == NULL) {
    benchmark_error("Failed to allocate memory.");
    goto failXit;
  }
  snprintf(stocks_file, size, "%s/%s", datafilesdir, STOCKS_FILE);

  size = strlen(datafilesdir) + strlen(CURRENCIES_FILE) + 2;
  currencies_file = malloc(size);
  if (currencies_file == NULL) {
    benchmark_error("Failed to allocate memory.");
    goto failXit;
  }
  snprintf(currencies_file, size, "%s/%s", datafilesdir, CURRENCIES_FILE);

  size = strlen(datafilesdir) + strlen(QUOTES_FILE) + 2;
  quotes_file = malloc(size);
  if (quotes_file == NULL) {
    benchmark_error("Failed to allocate memory.");
    goto failXit;
  }
  snprintf(quotes_file, size, "%s/%s", datafilesdir, QUOTES_FILE);
 
  if (benchmark_handle_alloc(&benchmarkP, 1, program, homedir, datafilesdir) != BENCHMARK_SUCCESS) {
    benchmark_error("Failed to allocate handle");
    goto failXit;
  }

  ret = load_personal_database(benchmarkP, personal_file);
  if (ret) {
    benchmark_error("Error loading personal database.");
    goto failXit;
  }

  ret = load_stocks_database(benchmarkP, stocks_file);
  if (ret) {
    benchmark_error("Error loading stocks database.");
    goto failXit;
  }

  ret = load_currencies_database(benchmarkP, currencies_file);
  if (ret) {
    benchmark_error("Error loading currencies database.");
    goto failXit;
  }

  ret = load_quotes_database(benchmarkP, quotes_file);
  if (ret) {
    benchmark_error("Error loading personal database.");
    goto failXit;
  }
 
  BENCHMARK_CLEAR_CREATE_DB(benchmarkP);

  if (benchmark_handle_free(benchmarkP) != BENCHMARK_SUCCESS) {
    benchmark_error("Failed to free handle");
    goto failXit;
  }

  benchmark_debug(1, "Done with initial load ...");

  ret = BENCHMARK_SUCCESS;
  goto cleanup;

 failXit:
  if (benchmark_handle_free(benchmarkP) != BENCHMARK_SUCCESS) {
    benchmark_error("Failed to free handle");
  }

  ret = BENCHMARK_FAIL;

cleanup:
  free(personal_file);
  free(stocks_file);
  free(currencies_file);
  free(quotes_file);

  return ret;
}


static int
load_personal_database(BENCHMARK_DBS *benchmarkP, const char *personal_file)
{
  int     rc = 0;
  int     cnt = 0;
  DBT     key, data;
  DB_TXN *txnP = NULL;
  DB_ENV  *envP = NULL;
  char buf[MAXLINE];
  FILE *ifp;
  PERSONAL my_personal;

  if (benchmarkP == NULL) {
    goto failXit;
  }
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL || personal_file == NULL) {
    benchmark_error("%s: Invalid arguments", __func__);
    goto failXit;
  }

  benchmark_info("-- Loading Personal database from: %s... ", personal_file);

  ifp = fopen(personal_file, "r");
  if (ifp == NULL) {
    benchmark_error("Error opening file '%s'", personal_file);
    goto failXit;
  }

  fprintf(stderr,"Inserted: %3d rows", 0); 

  /* Iterate over the vendor file */
  while (fgets(buf, MAXLINE, ifp) != NULL) {

    /* zero out the structure */
    memset(&my_personal, 0, sizeof(PERSONAL));

    /* Zero out the DBTs */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    /*
     * Scan the line into the structure.
     * Convenient, but not particularly safe.
     * In a real program, there would be a lot more
     * defensive code here.
     */
    sscanf(buf,
      "%10[^#]#%128[^#]#%128[^#]#%128[^#]#%128[^#]#%128[^#]#%128[^#]#%16[^\n]",
      my_personal.account_id, my_personal.last_name,
      my_personal.first_name, my_personal.address,
      my_personal.city,
      my_personal.state, my_personal.country,
      my_personal.phone);

    /* Now that we have our structure we can load it into the database. */

    /* Set up the database record's key */
    key.data = my_personal.account_id;
    key.size = (u_int32_t)strlen(my_personal.account_id) + 1;

    /* Set up the database record's data */
    data.data = &my_personal;
    data.size = sizeof(PERSONAL);

    /*
     * Note that given the way we built our struct, there's extra
     * bytes in it. Essentially we're using fixed-width fields with
     * the unused portion of some fields padded with zeros. This
     * is the easiest thing to do, but it does result in a bloated
     * database. 
     */

    cnt ++;
    /* Put the data into the database */
    benchmark_debug(4,"Inserting into Personal table (%d): %s", cnt, (char *)key.data);

    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction begin failed.", __LINE__, getpid());
      goto failXit; 
    }

    assert(txnP != NULL);
    assert(benchmarkP->personal_dbp != NULL);

    rc = benchmarkP->personal_dbp->put(benchmarkP->personal_dbp, txnP, &key, &data, DB_NOOVERWRITE);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Database put failed.", __LINE__, getpid());
      txnP->abort(txnP);
      goto failXit; 
    }

    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction commit failed.", __LINE__, getpid());
      goto failXit; 
    }
    fprintf(stderr,"\rInserted: %3d rows", cnt); 
  }
  fprintf(stderr, "\n");

  fclose(ifp);
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return BENCHMARK_SUCCESS;

failXit:
  if (ifp != NULL) {
    fclose(ifp);
  }

  return BENCHMARK_FAIL;
}

static int
load_stocks_database(BENCHMARK_DBS *benchmarkP, const char *stocks_file)
{
  int     rc = 0;
  int     cnt = 0;
  DBT key, data;
  DB_TXN *txnP = NULL;
  DB_ENV  *envP = NULL;
  char buf[MAXLINE];
  char ignore_buf[500];
  FILE *ifp;
  STOCK my_stocks;

  if (benchmarkP == NULL) {
    goto failXit;
  }
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL || stocks_file == NULL) {
    benchmark_error("%s: Invalid arguments", __func__);
    goto failXit;
  }

  benchmark_info("-- Loading Stocks database from: %s... ", stocks_file);

  ifp = fopen(stocks_file, "r");
  if (ifp == NULL) {
    benchmark_error("Error opening file '%s'", stocks_file);
    goto failXit;
  }

  fprintf(stderr,"Inserted: %3d rows", 0); 

  /* Iterate over the vendor file */
  while (fgets(buf, MAXLINE, ifp) != NULL) {

    /* zero out the structure */
    memset(&my_stocks, 0, sizeof(STOCK));

    /* Zero out the DBTs */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    /*
     * Scan the line into the structure.
     * Convenient, but not particularly safe.
     * In a real program, there would be a lot more
     * defensive code here.
     */
    sscanf(buf,
      "%10[^#]#%128[^#]#%500[^\n]",
      my_stocks.stock_symbol, my_stocks.full_name, ignore_buf);

    /* Now that we have our structure we can load it into the database. */

    /* Set up the database record's key */
    key.data = my_stocks.stock_symbol;
    key.size = (u_int32_t)strlen(my_stocks.stock_symbol) + 1;

    /* Set up the database record's data */
    data.data = &my_stocks;
    data.size = sizeof(STOCK);

    /*
     * Note that given the way we built our struct, there's extra
     * bytes in it. Essentially we're using fixed-width fields with
     * the unused portion of some fields padded with zeros. This
     * is the easiest thing to do, but it does result in a bloated
     * database.
     */ 

    /* Put the data into the database */
    cnt ++;
    benchmark_debug(4,"Inserting into Stocks table (%d): %s", cnt, (char *)key.data);
    benchmark_debug(4, "\t(%s, %s)", my_stocks.stock_symbol, my_stocks.full_name);

    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction begin failed.", __LINE__, getpid());
      goto failXit; 
    }

    rc = benchmarkP->stocks_dbp->put(benchmarkP->stocks_dbp, txnP, &key, &data, DB_NOOVERWRITE);

    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Database put failed.", __LINE__, getpid());
      txnP->abort(txnP);
      goto failXit; 
    }

    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction commit failed.", __LINE__, getpid());
      goto failXit; 
    }
    fprintf(stderr,"\rInserted: %3d rows", cnt); 
  }
  fprintf(stderr, "\n");

  fclose(ifp);
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return BENCHMARK_SUCCESS;

failXit:
  if (ifp != NULL) {
    fclose(ifp);
  }

  return BENCHMARK_FAIL;
}

static int
load_currencies_database(BENCHMARK_DBS *benchmarkP, const char *currencies_file)
{
  int     rc = 0;
  int     cnt = 0;
  DBT key, data;
  DB_TXN *txnP = NULL;
  DB_ENV  *envP = NULL;
  char buf[MAXLINE];
  FILE *ifp;
  CURRENCY my_currencies;

  if (benchmarkP == NULL) {
    goto failXit;
  }
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL || currencies_file == NULL) {
    benchmark_error("%s: Invalid arguments", __func__);
    goto failXit;
  }

  benchmark_info("-- Loading Currencies database from: %s... ", currencies_file);

  ifp = fopen(currencies_file, "r");
  if (ifp == NULL) {
    benchmark_error("Error opening file '%s'", currencies_file);
    goto failXit;
  }

  fprintf(stderr,"Inserted: %3d rows", 0); 

  /* Iterate over the vendor file */
  while (fgets(buf, MAXLINE, ifp) != NULL) {

    /* zero out the structure */
    memset(&my_currencies, 0, sizeof(CURRENCY));

    /* Zero out the DBTs */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    /*
     * Scan the line into the structure.
     * Convenient, but not particularly safe.
     * In a real program, there would be a lot more
     * defensive code here.
     */
    sscanf(buf,
      "%200[^#]#%200[^#]#%10[^\n]",
      my_currencies.country, my_currencies.currency_name, my_currencies.currency_symbol);

    /* Now that we have our structure we can load it into the database. */

    /* Set up the database record's key */
    key.data = my_currencies.currency_symbol;
    key.size = (u_int32_t)strlen(my_currencies.currency_symbol) + 1;

    /* Set up the database record's data */
    data.data = &my_currencies;
    data.size = sizeof(CURRENCY);

    /*
     * Note that given the way we built our struct, there's extra
     * bytes in it. Essentially we're using fixed-width fields with
     * the unused portion of some fields padded with zeros. This
     * is the easiest thing to do, but it does result in a bloated
     * database. 
     */

    /* Put the data into the database */
    cnt ++;
    benchmark_debug(4,"Inserting into Currencies table (%d): %s", cnt, (char *)key.data);

    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction begin failed.", __LINE__, getpid());
      goto failXit; 
    }

    rc = benchmarkP->currencies_dbp->put(benchmarkP->currencies_dbp, txnP, &key, &data, 0 /* Same currency for multiple contries*/);

    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Database put failed.", __LINE__, getpid());
      txnP->abort(txnP);
      goto failXit; 
    }

    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction commit failed.", __LINE__, getpid());
      goto failXit; 
    }

    fprintf(stderr,"\rInserted: %3d rows", cnt); 
  }
  fprintf(stderr, "\n");

  fclose(ifp);
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return BENCHMARK_SUCCESS;

failXit:
  if (ifp != NULL) {
    fclose(ifp);
  }

  return BENCHMARK_FAIL;
}

static int
load_quotes_database(BENCHMARK_DBS *benchmarkP, const char *quotes_file)
{
  int     rc = 0;
  int     current_slot = 0;
  int     cnt = 0;
  FILE   *ifp;
  DB_TXN *txnP = NULL;
  DB_ENV *envP = NULL;
  DBT     key, data;
  QUOTE   quote;
  char    buf[MAXLINE];

  if (benchmarkP == NULL) {
    goto failXit;
  }
  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL || benchmarkP->quotes_dbp == NULL || quotes_file == NULL) {
    benchmark_error( "%s: Invalid arguments", __func__);
    goto failXit;
  }

  benchmark_info("-- Loading Quotes database from: %s... ", quotes_file);

  ifp = fopen(quotes_file, "r");
  if (ifp == NULL) {
    benchmark_error("Error opening file '%s'", quotes_file);
    goto failXit;
  }

  fprintf(stderr,"Inserted: %3d rows", 0); 

  /* Iterate over the vendor file */
  while (fgets(buf, MAXLINE, ifp) != NULL) {

    /* zero out the structure */
    memset(&quote, 0, sizeof(QUOTE));

    /* Zero out the DBTs */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    /*
     * Scan the line into the structure.
     * Convenient, but not particularly safe.
     * In a real program, there would be a lot more
     * defensive code here.
     */
    sscanf(buf,
      "%10[^#]#%f#%10[^#]#%f#%f#%f#%f#%f#%ld#%10[^\n]",
      quote.symbol, &quote.current_price,
      quote.trade_time, &quote.low_price_day,
      &quote.high_price_day, &quote.perc_price_change, &quote.bidding_price,
      &quote.asking_price, &quote.trade_volume, quote.market_cap);

    /* Set all quotes to 500 to start with */
    quote.current_price = 500.0;

    /* Now that we have our structure we can load it into the database. */

    /* Set up the database record's key */
    key.data = quote.symbol;
    key.size = (u_int32_t)strlen(quote.symbol) + 1;

    /* Set up the database record's data */
    data.data = &quote;
    data.size = sizeof(QUOTE);

    /*
     * Note that given the way we built our struct, there's extra
     * bytes in it. Essentially we're using fixed-width fields with
     * the unused portion of some fields padded with zeros. This
     * is the easiest thing to do, but it does result in a bloated
     * database. 
     */

    /* Put the data into the database */
    cnt ++;
    benchmark_debug(6,"Inserting into Quotes table (%d): %s", cnt, (char *)key.data);

    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction begin failed.", __LINE__, getpid());
      goto failXit; 
    }

    rc = benchmarkP->quotes_dbp->put(benchmarkP->quotes_dbp, txnP, &key, &data, DB_NOOVERWRITE);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Database put failed.", __LINE__, getpid());
      txnP->abort(txnP);
      goto failXit; 
    }

    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "[%d] [%d] Transaction commit failed.", __LINE__, getpid());
      goto failXit; 
    }

    current_slot ++;
    fprintf(stderr,"\rInserted: %3d rows", cnt); 
  }
  fprintf(stderr, "\n");

  benchmarkP->number_stocks = current_slot;

  fclose(ifp);
  return BENCHMARK_SUCCESS;

failXit:
  if (ifp != NULL) {
    fclose(ifp);
  }

  return BENCHMARK_FAIL;
}


