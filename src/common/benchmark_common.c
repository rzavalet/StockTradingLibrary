/*
 * =====================================================================================
 *
 *       Filename:  benchmark_common.c
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

#include "benchmark_common.h"

static int
symbol_exists(const char *symbol, DB_TXN *txnP, BENCHMARK_DBS *benchmarkP);

static int
account_exists(const char *account_id, DB_TXN *txnP, BENCHMARK_DBS *benchmarkP);

static int 
show_stock_item(void *);

static int
show_currencies_item(void *vBuf);

static int
get_account_id(DB *sdbp,          /* secondary db handle */
              const DBT *pkey,   /* primary db record's key */
              const DBT *pdata,  /* primary db record's data */
              DBT *skey);         /* secondary db record's key */

static int 
create_portfolio(const char *account_id, 
                 const char *symbol, 
                 float price, 
                 int amount, 
                 int force_apply, 
                 DB_TXN *txnP, 
                 BENCHMARK_DBS *benchmarkP);

int
get_portfolio(const char *account_id, 
              const char *symbol, 
              DB_TXN *txnP, 
              DBC **cursorPP, 
              DBT *key_ret, 
              DBT *data_ret, 
              BENCHMARK_DBS *benchmarkP);

int
get_stock(const char *symbol, DB_TXN *txnP, DBC **cursorPP, DBT *key_ret, DBT *data_ret, int flags, BENCHMARK_DBS *benchmarkP);

/*=============== STATIC FUNCTIONS =======================*/
static int
get_account_id(DB *sdbp,          /* secondary db handle */
              const DBT *pkey,   /* primary db record's key */
              const DBT *pdata,  /* primary db record's data */
              DBT *skey)         /* secondary db record's key */
{
    PORTFOLIOS *portfoliosP;

    /* First, extract the structure contained in the primary's data */
    portfoliosP = pdata->data;

    /* Now set the secondary key's data to be the representative's name */
    memset(skey, 0, sizeof(DBT));
    skey->data = portfoliosP->account_id;
    skey->size = strlen(portfoliosP->account_id) + 1;

    /* Return 0 to indicate that the record can be created/updated. */
    return (0);
} 

static int
show_stock_item(void *vBuf)
{
  char *symbol;
  char *name;

  size_t buf_pos = 0;
  char *buf = (char *)vBuf;

  /* TODO: Pack the string instead */
  symbol = buf;
  buf_pos += ID_SZ;

  name = buf + buf_pos;

  /* Display all this information */
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "Symbol: %s", symbol);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tName: %s", name);

  /* Return the vendor's name */
  return 0;
}

static int
open_database(DB_ENV *envP,
              DB **dbpp,       
              const char *file_name,     
              const char *program_name,  
              FILE *error_file_pointer,
              int is_secondary,
              int create)
{
  DB *dbp;
  u_int32_t open_flags;
  int ret;

  /* Initialize the DB handle */
  ret = db_create(&dbp, envP, 0);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Could not create db handle.", __FILE__, __LINE__, getpid());
    return (ret);
  }
  /* Point to the memory malloc'd by db_create() */
  *dbpp = dbp;

  /* Set up error handling for this database */
  dbp->set_errfile(dbp, error_file_pointer);
  dbp->set_errpfx(dbp, program_name);

  /*
   * If this is a secondary database, then we want to allow
   * sorted duplicates.
   */
  if (is_secondary) {
    ret = dbp->set_flags(dbp, DB_DUPSORT);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Attempt to set DUPSORT flags failed.", __FILE__, __LINE__, getpid());
      return (ret);
    }
  }

  /* Set the open flags */
  open_flags = DB_THREAD          /* multi-threaded application */
              | DB_AUTO_COMMIT;   /* open is a transation */ 

  if (create) {
    open_flags |= DB_CREATE; /*  Allow database creation */
    open_flags |= DB_EXCL;   /*  Error if DB exists */
  }

  /* Now open the database */
  ret = dbp->open(dbp,        /* Pointer to the database */
                  NULL,       /* Txn pointer */
                  file_name,  /* File name */
                  NULL,       /* Logical db name */
                  DB_BTREE,   /* Database type (using btree) */
                  open_flags, /* Open flags */
                  0);         /* File mode. Using defaults */
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d]: Database '%s' open failed", __FILE__, __LINE__, getpid(), file_name);
    return (ret);
  }

  return (0);
}

static int
close_database(DB_ENV *envP,
              DB *dbP,       
              const char *program_name)
{
  int rc = 0;

  if (envP == NULL || dbP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  } 

  rc = dbP->close(dbP, 0);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Database close failed.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  goto cleanup;

failXit:
  rc = 1;

cleanup:
  return rc; 
}

int close_environment(BENCHMARK_DBS *benchmarkP)
{
  int rc = 0;
  DB_ENV  *envP = NULL;

  if (benchmarkP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }


  rc = envP->close(envP, 0);
  if (rc != 0) {
    benchmark_error("Error closing environment: %s", db_strerror(rc));
  }

  benchmarkP->envP = NULL;
  goto cleanup;

failXit:
  rc = 1;

cleanup:
  return rc;
}

int open_environment(BENCHMARK_DBS *benchmarkP)
{
  int rc = 0;
  u_int32_t env_flags;
  DB_ENV  *envP = NULL;

  if (benchmarkP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  rc = db_env_create(&envP, 0);
  if (rc != 0) {
    benchmark_error("Error creating environment handle: %s", db_strerror(rc));
    goto failXit;
  }
 
  env_flags = DB_INIT_TXN  |  /* Init transaction subsystem */
              DB_INIT_LOCK |  /* Init locking subsystem */
              DB_INIT_LOG  |  /* Init logging subsystem */
              DB_INIT_MPOOL|  /* Init shared memory buffer pool */
              DB_THREAD    ;  /* Multithreaded application */

  if (benchmarkP->createDBs == 1)  {
    env_flags |= DB_CREATE;   /* Create underlying files as necessary */
  }

 env_flags |= DB_SYSTEM_MEM;  /* Allocate from shared memory instead of heap memory */

  /*
   * Indicate that we want db to perform lock detection internally.
   * Also indicate that the transaction with the fewest number of
   * write locks will receive the deadlock notification in 
   * the event of a deadlock.
   */  
  rc = envP->set_lk_detect(envP, DB_LOCK_MINWRITE);
  if (rc != 0) {
      benchmark_error("Error setting lock detect: %s", db_strerror(rc));
      goto failXit;
  } 

  rc = envP->set_shm_key(envP, CHRONOS_SHMKEY); 
  if (rc != 0) {
      benchmark_error("Error setting shm key: %s", db_strerror(rc));
      goto failXit;
  } 

  rc = envP->set_timeout(envP, 10000000, DB_SET_LOCK_TIMEOUT);
  if (rc != 0) {
      benchmark_error("Error setting lock timeout: %s", db_strerror(rc));
      goto failXit;
  } 

  rc = envP->set_timeout(envP, 10000000, DB_SET_TXN_TIMEOUT);
  if (rc != 0) {
      benchmark_error("Error setting txn timeout: %s", db_strerror(rc));
      goto failXit;
  } 

  rc = envP->open(envP, benchmarkP->db_home_dir, env_flags, 0); 
  if (rc != 0) {
    benchmark_error("Error opening environment: %s", db_strerror(rc));
    goto failXit;
  }

  goto cleanup;

failXit:
  if (envP != NULL) {
    rc = envP->close(envP, 0);
    if (rc != 0) {
      benchmark_error("Error closing environment: %s", db_strerror(rc));
    }
  }
  rc = 1;

cleanup:
  benchmarkP->envP = envP;

  return rc;
}


/*=============== PUBLIC FUNCTIONS =======================*/
int	
databases_setup(BENCHMARK_DBS *benchmarkP,
                int which_database,
                const char *program_name, 
                FILE *error_fileP)
{
  int ret;
  DB_ENV  *envP = NULL;

  if (benchmarkP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  /* TODO: Maybe better to receive the envP and pass the home dir? */
  ret = open_environment(benchmarkP);
  if (ret != 0) {
    benchmark_error("Could not open environment");
    goto failXit;
  }

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  if (IS_STOCKS(which_database)) {
    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->stocks_dbp),
                        benchmarkP->stocks_db_name,
                        program_name, error_fileP,
                        PRIMARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }
  }

  if (IS_QUOTES(which_database)) {
    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->quotes_dbp),
                        benchmarkP->quotes_db_name,
                        program_name, error_fileP,
                        PRIMARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }
  }

  if (IS_QUOTES_HIST(which_database)) {
    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->quotes_hist_dbp),
                        benchmarkP->quotes_hist_db_name,
                        program_name, error_fileP,
                        PRIMARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }
  }

  if (IS_PORTFOLIOS(which_database)) {
    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->portfolios_dbp),
                        benchmarkP->portfolios_db_name,
                        program_name, error_fileP,
                        PRIMARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }

    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->portfolios_sdbp),
                        benchmarkP->portfolios_sdb_name,
                        program_name, error_fileP,
                        SECONDARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }

    /* Now associate the secondary to the primary */
    ret = benchmarkP->portfolios_dbp->associate(benchmarkP->portfolios_dbp,            /* Primary database */
                   NULL,           /* TXN id */
                   benchmarkP->portfolios_sdbp,           /* Secondary database */
                   get_account_id,
                   0);

    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Failed to associate secondary database.", __FILE__, __LINE__, getpid());
      return (ret);
    }

  }

  if (IS_ACCOUNTS(which_database)) {
    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->accounts_dbp),
                        benchmarkP->accounts_db_name,
                        program_name, error_fileP,
                        PRIMARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }
  }

  if (IS_CURRENCIES(which_database)) {
    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->currencies_dbp),
                        benchmarkP->currencies_db_name,
                        program_name, error_fileP,
                        PRIMARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }
  }

  if (IS_PERSONAL(which_database)) {
    ret = open_database(benchmarkP->envP,
                        &(benchmarkP->personal_dbp),
                        benchmarkP->personal_db_name,
                        program_name, error_fileP,
                        PRIMARY_DB,
                        benchmarkP->createDBs);
    if (ret != 0) {
      return (ret);
    }
  }

  return (0);

failXit:
  return 1;
}


int	
benchmark_end(BENCHMARK_DBS *benchmarkP,
              int which_database,
              char *program_name)
{
  int rc = 0;

  if (benchmarkP->envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  } 

  if (IS_STOCKS(which_database)) {
    rc = close_database(benchmarkP->envP,
                        benchmarkP->stocks_dbp,
                        program_name);
    if (rc != 0) {
      goto failXit;
    }
  }

  if (IS_QUOTES(which_database)) {
    rc = close_database(benchmarkP->envP,
                        benchmarkP->quotes_dbp,
                        program_name);
    if (rc != 0) {
      goto failXit;
    }
  }

  if (IS_QUOTES_HIST(which_database)) {
    rc = close_database(benchmarkP->envP,
                        benchmarkP->quotes_hist_dbp,
                        program_name);
    if (rc != 0) {
      goto failXit;
    }
  }

  if (IS_PORTFOLIOS(which_database)) {
    rc = close_database(benchmarkP->envP,
                        benchmarkP->portfolios_dbp,
                        program_name);
    if (rc != 0) {
      goto failXit;
    }
  }

  if (IS_ACCOUNTS(which_database)) {
    rc = close_database(benchmarkP->envP,
                        benchmarkP->accounts_dbp,
                        program_name);
    if (rc != 0) {
      goto failXit;
    }
  }

  if (IS_CURRENCIES(which_database)) {
    rc = close_database(benchmarkP->envP,
                        benchmarkP->currencies_dbp,
                        program_name);
    if (rc != 0) {
      goto failXit;
    }
  }

  if (IS_PERSONAL(which_database)) {
    rc = close_database(benchmarkP->envP,
                        benchmarkP->personal_dbp,
                        program_name);
    if (rc != 0) {
      goto failXit;
    }
  }

  return (0);

failXit:
  return 1; 
}

/* Initializes the STOCK_DBS struct.*/
void
initialize_benchmarkdbs(BENCHMARK_DBS *benchmarkP)
{
  memset(benchmarkP, 0, sizeof(BENCHMARK_DBS));
}

/* Identify all the files that will hold our databases. */
void
set_db_filenames(BENCHMARK_DBS *benchmarkP)
{
  size_t size;

  size = strlen(STOCKSDB) + 1;
  benchmarkP->stocks_db_name = malloc(size);
  snprintf(benchmarkP->stocks_db_name, size, "%s", STOCKSDB);

  size = strlen(QUOTESDB) + 1;
  benchmarkP->quotes_db_name = malloc(size);
  snprintf(benchmarkP->quotes_db_name, size, "%s", QUOTESDB);

  size = strlen(QUOTES_HISTDB) + 1;
  benchmarkP->quotes_hist_db_name = malloc(size);
  snprintf(benchmarkP->quotes_hist_db_name, size, "%s", QUOTES_HISTDB);

  size = strlen(PORTFOLIOSDB) + 1;
  benchmarkP->portfolios_db_name = malloc(size);
  snprintf(benchmarkP->portfolios_db_name, size, "%s", PORTFOLIOSDB);

  size = strlen(PORTFOLIOSSECDB) + 1;
  benchmarkP->portfolios_sdb_name = malloc(size);
  snprintf(benchmarkP->portfolios_sdb_name, size, "%s", PORTFOLIOSSECDB);

  size = strlen(ACCOUNTSDB) + 1;
  benchmarkP->accounts_db_name = malloc(size);
  snprintf(benchmarkP->accounts_db_name, size, "%s", ACCOUNTSDB);

  size = strlen(CURRENCIESDB) + 1;
  benchmarkP->currencies_db_name = malloc(size);
  snprintf(benchmarkP->currencies_db_name, size, "%s", CURRENCIESDB);

  size = strlen(PERSONALDB) + 1;
  benchmarkP->personal_db_name = malloc(size);
  snprintf(benchmarkP->personal_db_name, size, "%s", PERSONALDB);
}

int
databases_close(BENCHMARK_DBS *benchmarkP)
{
  int ret;

  DB_ENV  *envP = NULL;

  if (benchmarkP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (benchmarkP->stocks_dbp != NULL) {
    ret = benchmarkP->stocks_dbp->close(benchmarkP->stocks_dbp, 0);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Stocks database close failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  if (benchmarkP->quotes_dbp != NULL) {
    ret = benchmarkP->quotes_dbp->close(benchmarkP->quotes_dbp, 0);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Quotes database close failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  if (benchmarkP->quotes_hist_dbp != NULL) {
    ret = benchmarkP->quotes_hist_dbp->close(benchmarkP->quotes_hist_dbp, 0);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Quotes History database close failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  if (benchmarkP->portfolios_dbp != NULL) {
    ret = benchmarkP->portfolios_dbp->close(benchmarkP->portfolios_dbp, 0);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Portfolios database close failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  if (benchmarkP->accounts_dbp != NULL) {
    ret = benchmarkP->accounts_dbp->close(benchmarkP->accounts_dbp, 0);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Accounts database close failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  if (benchmarkP->currencies_dbp != NULL) {
    ret = benchmarkP->currencies_dbp->close(benchmarkP->currencies_dbp, 0);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Currencies database close failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  if (benchmarkP->personal_dbp != NULL) {
    ret = benchmarkP->personal_dbp->close(benchmarkP->personal_dbp, 0);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Personal database close failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return (0);

failXit:
  return 1;
}


int 
show_stocks_records(char *symbolId, BENCHMARK_DBS *benchmarkP)
{
  DBC *cursorP = NULL;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  DBT key, data;
  int ret;
  int rc = BENCHMARK_SUCCESS;

  if (benchmarkP==NULL|| symbolId == NULL)
  {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  if (symbolId != NULL && symbolId[0] != '\0') {
    key.data = symbolId;
    key.size = (u_int32_t)strlen(symbolId) + 1;
  }

  ret = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Transaction begin failed.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  ret = benchmarkP->stocks_dbp->cursor(benchmarkP->stocks_dbp, txnP,
                                    &cursorP, DB_READ_COMMITTED);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to create cursor for Stocks.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* Position the cursor */
  ret = cursorP->get(cursorP, &key, &data, DB_SET | DB_READ_COMMITTED | DB_RMW );
  if (ret != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to find record in Quotes.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  if (symbolId != NULL) {
    if (strcmp(symbolId, (char *)key.data) == 0) {
      (void) show_stock_item(data.data);
    }
  }
  else {
    (void) show_stock_item(data.data);
  }

  ret = cursorP->close(cursorP);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
  }
  cursorP = NULL;

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
  ret = txnP->commit(txnP, 0);
  if (ret != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Transaction commit failed. txnP: %p", __FILE__, __LINE__, getpid(), txnP);
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
        envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
      }
      cursorP = NULL;
    }

    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    ret = txnP->abort(txnP);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
    }
  }


cleanup:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return rc;
}

int 
show_all_portfolios(BENCHMARK_DBS *benchmarkP)
{
  DBC *cursorP = NULL;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  char *symbolIdP = NULL;
  DBT key, data;
  int ret;
  int rc = BENCHMARK_SUCCESS;
  int curRc = 0;

  if (benchmarkP == NULL || benchmarkP->personal_dbp == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  ret = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Transaction begin failed.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* First read the portfolios account */
  ret = benchmarkP->portfolios_dbp->cursor(benchmarkP->portfolios_dbp, txnP,
                                    &cursorP, DB_READ_COMMITTED);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to create cursor for Portfolio.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  while ((curRc=cursorP->get(cursorP, &key, &data, DB_READ_COMMITTED | DB_NEXT)) == 0)
  {
    (void) show_portfolio_item(data.data, &symbolIdP);
  }

  ret = cursorP->close(cursorP);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
  }
  cursorP = NULL;

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
  ret = txnP->commit(txnP, 0);
  if (ret != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Transaction commit failed. txnP: %p", __FILE__, __LINE__, getpid(), txnP);
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
        envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
      }
      cursorP = NULL;
    }

    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    ret = txnP->abort(txnP);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
    }
  }

cleanup:
  return (rc);
}

int 
show_portfolios(char              *account_id, 
                int               showOnlyUsers, 
                benchmark_xact_h  xactH,
                BENCHMARK_DBS     *benchmarkP)
{
  DBC *personal_cursorP = NULL;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  DBT key, data;
  int ret;
  int rc = BENCHMARK_SUCCESS;
  int curRc = 0;
  int numClients = 0;

  if (benchmarkP == NULL || benchmarkP->personal_dbp == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  if (xactH == NULL) {
    ret = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Transaction begin failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }
  else {
    txnP = (DB_TXN *)xactH;
  }

  /* First read the personall account */
  ret = benchmarkP->personal_dbp->cursor(benchmarkP->personal_dbp, txnP,
                                    &personal_cursorP, DB_READ_COMMITTED);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to create cursor for Personal.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* There are two cases, depending on whether the user id is provided or not.
   *
   * 1) If an user is provided, then only position the cursor on that user's record.
   * 2) If a user is NOT provided, then let's print out information for every user.
   */
  if (account_id != NULL && account_id[0] != '\0') {
    key.data = account_id;
    key.size = (u_int32_t) strlen(account_id) + 1;
    curRc=personal_cursorP->get(personal_cursorP, &key, &data, DB_SET | DB_READ_COMMITTED);
    if (curRc == 0) {

      /* Show user's information */
      (void) show_personal_item(data.data);   

      if (!showOnlyUsers) {
        /* Now display his portfolios */
        ret = show_one_portfolio(key.data, txnP, benchmarkP);
        if (ret != BENCHMARK_SUCCESS) {
          benchmark_error("Failed to retrieve portfolio");
          //goto failXit;
        }
      }
      numClients ++;

      
    }
  }
  else {
    while ((curRc=personal_cursorP->get(personal_cursorP, &key, &data, DB_READ_COMMITTED | DB_NEXT)) == 0)
    {
      /* Show user's information */
      (void) show_personal_item(data.data);   

      if (!showOnlyUsers) {
        /* Now display his portfolios */
        ret = show_one_portfolio(key.data, txnP, benchmarkP);
        if (ret != BENCHMARK_SUCCESS) {
          benchmark_error("Failed to retrieve portfolio");
          //goto failXit;
        }
      }
      numClients ++;
    }

    if (curRc != DB_NOTFOUND) {
      envP->err(envP, ret, "[%s:%d] [%d] Error retrieving portfolios.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  ret = personal_cursorP->close(personal_cursorP);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
  }
  personal_cursorP = NULL;

  if (xactH == NULL) {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
    ret = txnP->commit(txnP, 0);
    if (ret != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction commit failed. txnP: %p", __FILE__, __LINE__, getpid(), txnP);
      goto failXit; 
    }
    txnP = NULL;
  }

  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (xactH == NULL && txnP != NULL) {
    if (personal_cursorP != NULL) {
      ret = personal_cursorP->close(personal_cursorP);
      if (ret != 0) {
        envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
      }
      personal_cursorP = NULL;
    }

    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    ret = txnP->abort(txnP);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
    }
  }

  rc = BENCHMARK_FAIL;
cleanup:
  return (rc);
}

/* 
 * Given an account_id belonging to a user, show all the symbols associated
 * with that user.
 *
 * PARAMETERS:
 *    account_id      (IN) The account id we want to explore
 *    txn_inP         (IN) A transaction could be already open. In that case,
 *                         there is no need to create a new one.
 *    benchmarkP      (IN) Pointer to the benchmark context
 */
int
show_one_portfolio(char *account_id, DB_TXN  *txn_inP, BENCHMARK_DBS *benchmarkP)
{
  DBC *portfolio_cursorP = NULL;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  DBT key;
  DBT pkey, pdata;
  char *symbolIdP = NULL;
  int rc = BENCHMARK_SUCCESS;
  int ret;
  int numPortfolios = 0;

  if (benchmarkP == NULL || benchmarkP->portfolios_sdbp == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  if (txn_inP != NULL) {
    txnP = txn_inP;
  }
  else {
    ret = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (ret != 0) {
      envP->err(envP, ret, "[%s:%d] [%d] Transaction begin failed.", __FILE__, __LINE__, getpid());
      goto failXit;
    }
  }

  memset(&key, 0, sizeof(DBT));
  memset(&pkey, 0, sizeof(DBT));
  memset(&pdata, 0, sizeof(DBT));

  key.data = account_id;
  key.size = ID_SZ;
 
  /* Create a cursor to iterate over the portfolios given the 
   * user id. */
  ret = benchmarkP->portfolios_sdbp->cursor(benchmarkP->portfolios_sdbp, txnP, 
                                   &portfolio_cursorP, DB_READ_COMMITTED);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to create cursor for Portfolios.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, %p : searching for account id: %s", getpid(), txnP, account_id);

#if 1
  /* Iterate over all portfolios in order to find those belonging to the user_id */
  while ((ret = portfolio_cursorP->pget(portfolio_cursorP, &key, &pkey, &pdata, DB_NEXT)) == 0)
  {
    /* TODO: Is it necessary this comparison? */
    if (strcmp(account_id, (char *)key.data) == 0) {
      /* Finally, go ahead and display the information about this
       * portfolio */
      (void) show_portfolio_item(pdata.data, &symbolIdP);

      numPortfolios ++;
    }
  }

#else 

  /* Position the cursor */
  rc = portfolio_cursorP->pget(portfolio_cursorP, &key, &pkey, &pdata, DB_SET | DB_READ_COMMITTED);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to find record (%s) in Portfolio.", __FILE__, __LINE__, getpid(), account_id);
    goto failXit;
  }

  (void) show_portfolio_item(pdata.data, &symbolIdP);

#endif
  ret = portfolio_cursorP->close(portfolio_cursorP);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
  }

  portfolio_cursorP = NULL;

  /* This means this function created its own txn */
  if (txn_inP == NULL) {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
    ret = txnP->commit(txnP, 0);
    if (ret != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction commit failed. txnP: %p", __FILE__, __LINE__, getpid(), txnP);
      goto failXit; 
    }
    txnP = NULL;
  }

  goto cleanup;

failXit:
  rc = BENCHMARK_FAIL;
  if (txnP != NULL) {
    if (portfolio_cursorP != NULL) {
      ret = portfolio_cursorP->close(portfolio_cursorP);
      if (ret != 0) {
        envP->err(envP, ret, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
      }
      portfolio_cursorP = NULL;
    }

    /* This means this function created its own txn */
    if (txn_inP == NULL) {
      benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
      ret = txnP->abort(txnP);
      if (ret != 0) {
        envP->err(envP, ret, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
      }
    }
  }

cleanup:
  return (rc);
}

int
show_personal_item(void *vBuf)
{
  char      *account_id;
  char      *last_name;
  char      *first_name;
  char      *address;
  char      *address_2;
  char      *city;
  char      *state;
  char      *country;
  char      *phone;
  char      *email;

  size_t buf_pos = 0;
  char *buf = (char *)vBuf;

  account_id = buf;
  buf_pos += ID_SZ;
  
  last_name = buf + buf_pos;
  buf_pos += NAME_SZ;

  first_name = buf + buf_pos;
  buf_pos += NAME_SZ;

  address = buf + buf_pos;
  buf_pos += NAME_SZ;

  address_2 = buf + buf_pos;
  buf_pos += NAME_SZ;

  city = buf + buf_pos;
  buf_pos += NAME_SZ;

  state = buf + buf_pos;
  buf_pos += NAME_SZ;

  country = buf + buf_pos;
  buf_pos += NAME_SZ;

  phone = buf + buf_pos;
  buf_pos += NAME_SZ;

  email = buf + buf_pos;
 
  /* Display all this information */
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "================= SHOWING PERSON ==============");
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "AccountId: %s", account_id);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tLast Name: %s", last_name);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tFirst Name: %s", first_name);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tAddress: %s", address);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tAdress 2: %s", address_2);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tCity: %s", city);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tState: %s", state);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tCountry: %s", country);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tPhone: %s", phone);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tEmail: %s", email);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "==================================================\n");

  return 0;
}


int 
show_currencies_records(BENCHMARK_DBS *my_benchmarkP)
{
  DBC *currencies_cursorp = NULL;
  DBT key, data;
  int exit_value, ret;

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "================= SHOWING CURRENCIES DATABASE ==============\n");

  my_benchmarkP->currencies_dbp->cursor(my_benchmarkP->currencies_dbp, NULL,
                                    &currencies_cursorp, 0);

  exit_value = 0;
  while ((ret =
    currencies_cursorp->get(currencies_cursorp, &key, &data, DB_NEXT)) == 0)
  {
    (void) show_currencies_item(data.data);
  }

  currencies_cursorp->close(currencies_cursorp);
  return (exit_value);
}

static int
show_currencies_item(void *vBuf)
{
  char      *currency_symbol;
  int       exchange_rate_usd;
  char      *country;
  char      *currency_name;

  size_t buf_pos = 0;
  char *buf = (char *)vBuf;

  currency_symbol = buf;
  buf_pos += ID_SZ;
  
  country = buf + buf_pos;
  buf_pos += LONG_NAME_SZ;

  currency_name = buf + buf_pos;
  buf_pos += NAME_SZ;

  exchange_rate_usd = *((int *)(buf + buf_pos));

  /* Display all this information */
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "Currency Symbol: %s", currency_symbol);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tExchange Rate: %d", exchange_rate_usd);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tCountry: %s", country);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tName: %s", currency_name);

  return 0;
}

int
show_portfolio_item(void *vBuf, char **symbolIdPP)
{
  PORTFOLIOS *portfolioP;

  portfolioP = (PORTFOLIOS *)vBuf;
  /* Display all this information */
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "================= SHOWING PORTFOLIO ==============");
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "Portfolio ID: %s", portfolioP->portfolio_id);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tAccount ID: %s", portfolioP->account_id);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tSymbol ID: %s", portfolioP->symbol);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\t# Stocks Hold: %d", portfolioP->hold_stocks);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tSell?: %d", portfolioP->to_sell);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\t# Stocks to sell: %d", portfolioP->number_sell);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tPrice to sell: %d", portfolioP->price_sell);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tBuy?: %d", portfolioP->to_buy);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\t# Stocks to buy: %d", portfolioP->number_buy);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "\tPrice to buy: %d", portfolioP->price_buy);
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_OP, "==================================================\n");

  if (symbolIdPP) {
    *symbolIdPP = portfolioP->symbol; 
  }

  return 0;
}

int 
start_xact(benchmark_xact_h *xact_ret, const char *txn_name, BENCHMARK_DBS *benchmarkP)
{
  int rc = BENCHMARK_SUCCESS;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;

  if (benchmarkP == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Transaction begin failed.", __FILE__, __LINE__, getpid());
    goto failXit; 
  }
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Starting transaction: %p", getpid(), txnP);

  rc = txnP->set_name(txnP, txn_name);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Transaction name set failed.", __FILE__, __LINE__, getpid());
    goto failXit; 
  }

  *xact_ret = txnP;

  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (txnP != NULL) {
    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    rc = txnP->abort(txnP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
    }
  }

  *xact_ret = NULL;

  rc = BENCHMARK_FAIL;

cleanup:
  return rc;
}

int 
commit_xact(benchmark_xact_h xactH, BENCHMARK_DBS *benchmarkP)
{
  int rc = BENCHMARK_SUCCESS;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;

  if (benchmarkP == NULL || xactH == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  txnP = (DB_TXN *)xactH;

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
  rc = txnP->commit(txnP, 0);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Transaction commit failed. txnP: %p", __FILE__, __LINE__, getpid(), txnP);
    goto failXit; 
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (txnP != NULL) {
    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    rc = txnP->abort(txnP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
    }
  }

  rc = BENCHMARK_FAIL;

cleanup:
  return rc;
}

int 
abort_xact(benchmark_xact_h xactH, BENCHMARK_DBS *benchmarkP)
{
  int rc = BENCHMARK_SUCCESS;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;

  if (benchmarkP == NULL || xactH == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  txnP = (DB_TXN *)xactH;

  benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
  rc = txnP->abort(txnP);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  rc = BENCHMARK_FAIL;

cleanup:
  return rc;
}

int 
show_quote(char *symbolP, benchmark_xact_h xactH, BENCHMARK_DBS *benchmarkP)
{
  int rc = BENCHMARK_SUCCESS;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  DBT      key, data;
  DBC     *cursorp = NULL; /* To iterate over the porfolios */
  //QUOTE   *quoteP = NULL;

  if (benchmarkP == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  if (xactH == NULL) {
    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction begin failed.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }
  }
  else {
    txnP = (DB_TXN *)xactH;
  }

  rc = get_stock(symbolP, txnP, &cursorp, &key, &data, 0, benchmarkP);
  if (rc != BENCHMARK_SUCCESS) {
    benchmark_error("Could not find record.");
    goto failXit; 
  }
  
  //quoteP = data.data;
  //benchmark_info("*** Value of %s is %f", quoteP->symbol, quoteP->current_price);

  /* Close the record */
  if (cursorp != NULL) {
    rc = cursorp->close(cursorp);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Failed to close cursor for quote", __FILE__, __LINE__, getpid());
      goto failXit; 
    }
    cursorp = NULL;
  }

  if (xactH == NULL) {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction commit failed. txnP: %p", __FILE__, __LINE__, getpid(), txnP);
      goto failXit; 
    }
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (xactH == NULL && txnP != NULL) {
    if (cursorp != NULL) {
      rc = cursorp->close(cursorp);
      if (rc != 0) {
        envP->err(envP, rc, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
      }
      cursorp = NULL;
    }

    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    rc = txnP->abort(txnP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
    }
  }

  rc = BENCHMARK_FAIL;

cleanup:
  return rc;
}

/*-----------------------------------------------
 * Update the price of the stock for the provided
 * symbol with the provided new value.
 *
 * We first get the stock, then update the 
 * structure with the new price and then
 * save the changes in the database.
 *---------------------------------------------*/
int 
update_stock(char             *symbolP, 
             float             newValue, 
             benchmark_xact_h  xactH,
             BENCHMARK_DBS    *benchmarkP)
{
  int rc = BENCHMARK_SUCCESS;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  DBT      key, data;
  DBC     *cursorp = NULL; /* To iterate over the porfolios */
  QUOTE   *quoteP = NULL;

  if (benchmarkP == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  if (xactH == NULL) {
    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction begin failed.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }
  }
  else {
    txnP = (DB_TXN *)xactH;
  }

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT,"PID: %d, Starting transaction: %p", getpid(), txnP);
  rc = get_stock(symbolP, txnP, &cursorp, &key, &data, DB_RMW, benchmarkP);
  if (rc != BENCHMARK_SUCCESS) {
    benchmark_error("Could not find record.");
    goto failXit; 
  }
  
  /* Update whatever we need to update */
  quoteP = data.data;
  if (newValue >= 0) {
    quoteP->current_price = newValue;
  }
  else {
    int direction = rand() % 2;
    if (direction == 0 || quoteP->current_price <= 0) {
      quoteP->current_price += 0.1;
    }
    else {
      quoteP->current_price -= 0.1;
    }
  }

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, txnP: %p Updating %s to %f", getpid(), txnP, quoteP->symbol, quoteP->current_price);

  /* Save the record */
  rc = cursorp->put(cursorp, &key, &data, DB_CURRENT);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] failed to update quote", __FILE__, __LINE__, getpid());
    goto failXit; 
  }

  /* Close the record */
  if (cursorp != NULL) {
    rc = cursorp->close(cursorp);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Failed to close cursor for quote", __FILE__, __LINE__, getpid());
      goto failXit; 
    }
    cursorp = NULL;
  }

  if (xactH == NULL) {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction commit failed. txnP: %p", __FILE__, __LINE__, getpid(), txnP);
      goto failXit; 
    }
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (xactH == NULL && txnP != NULL) {
    if (cursorp != NULL) {
      rc = cursorp->close(cursorp);
      if (rc != 0) {
        envP->err(envP, rc, "[%s:%d] [%d] Failed to close cursor.", __FILE__, __LINE__, getpid());
      }
      cursorp = NULL;
    }

    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    rc = txnP->abort(txnP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
    }
  }

  rc = BENCHMARK_FAIL;

cleanup:
  return rc;
}


int 
sell_stocks(const char *account_id, 
            const char *symbol, 
            float price, 
            int amount, 
            int force_apply, 
            benchmark_xact_h  xactH,
            BENCHMARK_DBS *benchmarkP)
{
  int rc = 0;
  DB_TXN  *txnP = NULL;
  DB_ENV  *envP = NULL;
  DBT      key_portfolio, data_portfolio;
  DBT      key_quote, data_quote;
  DBC     *cursor_portfolioP = NULL; /* To iterate over the porfolios */
  DBC     *cursor_primary_portfolioP = NULL; /* To iterate over the porfolios */
  DBC     *cursor_quoteP = NULL; /* To iterate over the quotes */
  int      exists = 0;
  PORTFOLIOS *portfolioP = NULL;
  QUOTE      *quoteP = NULL;

  if (benchmarkP == NULL) {
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  memset(&key_portfolio, 0, sizeof(DBT));
  memset(&data_portfolio, 0, sizeof(DBT));
  memset(&key_quote, 0, sizeof(DBT));
  memset(&data_quote, 0, sizeof(DBT));

  if (xactH == NULL) {
    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "Transaction begin failed.");
      goto failXit; 
    }
  }
  else {
    txnP = (DB_TXN *)xactH;
  }

  /* 1) search the account */
  exists = account_exists(account_id, txnP, benchmarkP);
  if (!exists) {
    benchmark_error("This user account (%s) does not exist.", account_id);
    goto failXit; 
  }

  /* 2) search the symbol */
  exists = symbol_exists(symbol, txnP, benchmarkP);
  if (!exists) {
    benchmark_error("This symbol (%s) does not exist.", symbol);
    goto failXit; 
  }

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Looking up portfolio for account: %s and symbol: %s", account_id, symbol);

  /* get a cursor to the portfolio */
  rc = get_portfolio(account_id, symbol, txnP, &cursor_portfolioP, &key_portfolio, &data_portfolio, benchmarkP);
  if (rc != BENCHMARK_SUCCESS) {
    benchmark_error("Failed to obtain portfolio for account: %s and symbol: %s.", account_id, symbol);
    goto failXit; 
  }

  /* Update whatever we need to update */
  portfolioP = data_portfolio.data;
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Found portfolio for account: %s and symbol: %s -> %s", account_id, symbol, portfolioP->portfolio_id);
  if (portfolioP->hold_stocks < amount) {
    benchmark_error("Not enough stocks for this symbol. Have: %d, wanted: %d", portfolioP->hold_stocks, amount);
    goto failXit; 
  }

  /* Locate the porfolio in the primary database */
  rc = benchmarkP->portfolios_dbp->cursor(benchmarkP->portfolios_dbp, txnP,
                                    &cursor_primary_portfolioP, DB_READ_COMMITTED);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to create cursor for Portfolio.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  rc = cursor_primary_portfolioP->get(cursor_primary_portfolioP, &key_portfolio, &data_portfolio, DB_SET);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to find record in Portfolio.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  portfolioP = data_portfolio.data;

  /* Perform the sell right away */
  if (force_apply == 1) {
    rc = get_stock(symbol, txnP, &cursor_quoteP, &key_quote, &data_quote, 0, benchmarkP);
    if (rc != BENCHMARK_SUCCESS) {
      benchmark_error("Could not find record.");
      goto failXit; 
    }
    quoteP = data_quote.data;
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Current price for stock: %s is %f, requested is: %f", symbol, quoteP->current_price, price);
    if (quoteP->current_price >= price) {
      benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Selling %d stocks", amount);
      portfolioP->hold_stocks -= amount;
    }
    else {
      benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Price is to low to process request");
      goto failXit;
    }
  }
  /* Save the request and let the system decide */
  else {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Setting sell request for stock: %s for %d at %f", symbol, amount, price);
    portfolioP->to_sell = 1;
    portfolioP->number_sell = amount;
    portfolioP->price_sell = price;
  }

  /* Save the record */
  rc = cursor_primary_portfolioP->put(cursor_primary_portfolioP, &key_portfolio, &data_portfolio, DB_CURRENT);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Could not update record.", __FILE__, __LINE__, getpid());
    goto failXit; 
  }

  /* Close the record */
  if (cursor_portfolioP != NULL) {
    rc = cursor_portfolioP->close(cursor_portfolioP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }

    cursor_portfolioP = NULL;
  }

  /* Close the record */
  if (cursor_primary_portfolioP != NULL) {
    rc = cursor_primary_portfolioP->close(cursor_primary_portfolioP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }

    cursor_primary_portfolioP = NULL;
  }

  if (xactH == NULL) {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "%s:%d Transaction commit failed.", __func__, __LINE__);
      goto failXit; 
    }
    txnP = NULL;
  }

  rc = BENCHMARK_SUCCESS;
  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (xactH == NULL && txnP != NULL) {
    if (cursor_portfolioP != NULL) {
      rc = cursor_portfolioP->close(cursor_portfolioP);
      if (rc != 0) {
        envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
      }
      cursor_portfolioP = NULL;
    }

    if (cursor_primary_portfolioP != NULL) {
      rc = cursor_primary_portfolioP->close(cursor_primary_portfolioP);
      if (rc != 0) {
        envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
        goto failXit; 
      }

      cursor_primary_portfolioP = NULL;
    }

    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    rc = txnP->abort(txnP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Transaction abort failed.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }
  }

  rc = BENCHMARK_FAIL;

cleanup:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return rc;
}

int 
place_order(const char *account_id, 
            const char *symbol, 
            float price, 
            int amount, 
            int force_apply, 
            benchmark_xact_h  xactH,
            BENCHMARK_DBS *benchmarkP)
{
  int rc = 0;
  int exists = 0;
  DBT      key_portfolio, data_portfolio;
  DBT      key_quote, data_quote;
  DBC     *cursor_portfolioP = NULL; /* To iterate over the porfolios */
  DBC     *cursor_primary_portfolioP = NULL; /* To iterate over the porfolios */
  DBC     *cursor_quoteP = NULL; /* To iterate over the quotes */
  DB_TXN *txnP = NULL;
  DB_ENV  *envP = NULL;
  DBT key, data;
  PORTFOLIOS *portfolioP = NULL;
  QUOTE      *quoteP = NULL;

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  if (xactH == NULL) {
    rc = envP->txn_begin(envP, NULL, &txnP, DB_READ_COMMITTED | DB_TXN_WAIT);
    if (rc != 0) {
      envP->err(envP, rc, "Transaction begin failed.");
      goto failXit; 
    }
  }
  else {
    txnP = (DB_TXN *)xactH;
  }

  /* 1) search the account */
  exists = account_exists(account_id, txnP, benchmarkP);
  if (!exists) {
    benchmark_error("This user account (%s) does not exist.", account_id);
    goto failXit; 
  }

  /* 2) search the symbol */
  exists = symbol_exists(symbol, txnP, benchmarkP);
  if (!exists) {
    benchmark_error("This symbol (%s) does not exist.", symbol);
    goto failXit; 
  }

  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Looking up portfolio for account: %s and symbol: %s", account_id, symbol);

  /* 3) exists portfolio */
  rc = get_portfolio(account_id, symbol, txnP, &cursor_portfolioP, &key_portfolio, &data_portfolio, benchmarkP);

  /* 3.1) if so, update */
  if (rc == BENCHMARK_SUCCESS) {
    rc = benchmarkP->portfolios_dbp->cursor(benchmarkP->portfolios_dbp, txnP,
                                      &cursor_primary_portfolioP, DB_READ_COMMITTED);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Failed to create cursor for Portfolio.", __FILE__, __LINE__, getpid());
      goto failXit;
    }

    rc = cursor_primary_portfolioP->get(cursor_primary_portfolioP, &key_portfolio, &data_portfolio, DB_SET);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Failed to find record in Portfolio.", __FILE__, __LINE__, getpid());
      goto failXit;
    }

    portfolioP = data_portfolio.data;

    /* Perform the sell right away */
    if (force_apply == 1) {
      rc = get_stock(symbol, txnP, &cursor_quoteP, &key_quote, &data_quote, 0, benchmarkP);
      if (rc != BENCHMARK_SUCCESS) {
        benchmark_error("Could not find record.");
        goto failXit; 
      }
      quoteP = data_quote.data;
      benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Current price for stock: %s is %f, requested is: %f", symbol, quoteP->current_price, price);
      if (quoteP->current_price <= price) {
        benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Purchasing %d stocks", amount);
        portfolioP->hold_stocks += amount;
      }
      else {
        benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Price is to high to process request");
        goto failXit;
      }
    }
    /* Save the request and let the system decide */
    else {
      benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Setting buy request for stock: %s for %d at %f", symbol, amount, price);
      portfolioP->to_buy = 1;
      portfolioP->number_buy = amount;
      portfolioP->price_buy = price;
    }

    /* Save the record */

    rc = cursor_primary_portfolioP->put(cursor_primary_portfolioP, &key_portfolio, &data_portfolio, DB_CURRENT);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Could not update record.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }

  }
  /* 3.2) otherwise, create a new portfolio */
  else {
    if (force_apply == 1) {
      rc = get_stock(symbol, txnP, &cursor_quoteP, &key_quote, &data_quote, 0, benchmarkP);
      if (rc != BENCHMARK_SUCCESS) {
        benchmark_error("Could not find record.");
        goto failXit; 
      }
      quoteP = data_quote.data;
      benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Current price for stock: %s is %f, requested is: %f", symbol, quoteP->current_price, price);
      if (quoteP->current_price <= price) {
        benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Purchasing %d stocks of symbol: %s at %f USD since %s wanted a price <= %f USD", 
                       amount, symbol, quoteP->current_price, account_id, price);
      }
      else {
        benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Price is to high to process request. Price is: %f USD for symbol: %s, but %s wanted a price <= %f USD ",
                        quoteP->current_price, symbol, account_id, price);
        goto failXit;
      }
    }

    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "User: %s currently doesn't hold stocks of symbol: %s, so updating its portfolio....", account_id, symbol);
    rc = create_portfolio(account_id, symbol, price, amount, force_apply, txnP, benchmarkP);
    if (rc != BENCHMARK_SUCCESS) {
      benchmark_error("Could not create new entry in portfolio");
      goto failXit; 
    }
  }

  /* Close the record */
  if (cursor_portfolioP != NULL) {
    rc = cursor_portfolioP->close(cursor_portfolioP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }

    cursor_portfolioP = NULL;
  }

  /* Close the record */
  if (cursor_primary_portfolioP != NULL) {
    rc = cursor_primary_portfolioP->close(cursor_primary_portfolioP);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
      goto failXit; 
    }

    cursor_primary_portfolioP = NULL;
  }

  if (xactH == NULL) {
    benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, Committing transaction: %p", getpid(), txnP);
    rc = txnP->commit(txnP, 0);
    if (rc != 0) {
      envP->err(envP, rc, "%s:%d Transaction commit failed.", __func__, __LINE__);
      goto failXit; 
    }
    txnP = NULL;
  }

  goto cleanup;

failXit:
  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (xactH == NULL && txnP != NULL) {
    if (cursor_portfolioP != NULL) {
      rc = cursor_portfolioP->close(cursor_portfolioP);
      if (rc != 0) {
        envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
      }
      cursor_portfolioP = NULL;
    }

    if (cursor_primary_portfolioP != NULL) {
      rc = cursor_primary_portfolioP->close(cursor_primary_portfolioP);
      if (rc != 0) {
        envP->err(envP, rc, "[%s:%d] [%d] Could not close cursor.", __FILE__, __LINE__, getpid());
        goto failXit; 
      }

      cursor_primary_portfolioP = NULL;
    }

    benchmark_warning("PID: %d About to abort transaction. txnP: %p", getpid(), txnP);
    rc = txnP->abort(txnP);
    if (rc != 0) {
      envP->err(envP, rc, "Transaction abort failed.");
      goto failXit; 
    }
  }

  rc = 1;

cleanup:
  return rc;
}

int
get_portfolio(const char *account_id, 
              const char *symbol, 
              DB_TXN *txnP, 
              DBC **cursorPP, 
              DBT *key_ret, 
              DBT *data_ret, 
              BENCHMARK_DBS *benchmarkP) 
{
  DBC *cursorp = NULL;
  DB  *portfoliosdbP= NULL;
  DB  *portfoliossdbP= NULL;
  DB_ENV  *envP = NULL;
  DBT pkey, pdata;
  DBT key;
  int rc = 0;

  if (account_id == NULL || account_id[0] == '\0' || 
      symbol == NULL || symbol[0] == '\0' || 
      txnP == NULL || benchmarkP == NULL) 
  {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  portfoliosdbP = benchmarkP->portfolios_dbp;
  if (portfoliosdbP == NULL) {
    benchmark_error("Portfolios database is not open");
    goto failXit;
  }

  portfoliossdbP = benchmarkP->portfolios_sdbp;
  if (portfoliossdbP == NULL) {
    benchmark_error("Portfolios secondary database is not open");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&pkey, 0, sizeof(DBT));
  memset(&pdata, 0, sizeof(DBT));

  key.data = (char *)account_id;
  key.size = ID_SZ;

  rc = portfoliossdbP->cursor(portfoliossdbP, txnP,
                         &cursorp, 0);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to create cursor for Portfolios.", __FILE__, __LINE__, getpid());
    goto failXit;
  }
  
  while ((rc=cursorp->pget(cursorp, &key, &pkey, &pdata, DB_NEXT)) == 0) {
    /* TODO: Is this comparison needed? */
    if (strcmp(account_id, (char *)key.data) == 0) {
      if ( symbol != NULL && symbol[0] != '\0') {
        if (strcmp(symbol, ((PORTFOLIOS *)pdata.data)->symbol) == 0) {
          rc = BENCHMARK_SUCCESS;
          goto cleanup;
        }
      }
    }
  }

failXit:
  benchmark_warning("Could not find symbol %s for account_id: %s", symbol, account_id);

  rc = cursorp->close(cursorp);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to close cursor for Portfolios.", __FILE__, __LINE__, getpid());
  }
  cursorp = NULL;

  rc = BENCHMARK_FAIL;
  return rc;

cleanup:
  *cursorPP = cursorp;
  if (key_ret != NULL) {
    *key_ret = pkey;
  }

  if (data_ret != NULL) {
    *data_ret = pdata;
  }

  return rc;
}

int
get_stock(const char *symbol, DB_TXN *txnP, DBC **cursorPP, DBT *key_ret, DBT *data_ret, int flags, BENCHMARK_DBS *benchmarkP) 
{
  DBC *cursorp = NULL;
  DB  *quotesdbP= NULL;
  DB_ENV  *envP = NULL;
  QUOTE   *quoteP = NULL;
  DBT key, data;
  int rc = 0;

  if (benchmarkP==NULL || txnP == NULL || cursorPP == NULL || symbol == NULL || symbol[0] == '\0')
  {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid argument");
    goto failXit;
  }

  quotesdbP = benchmarkP->quotes_dbp;
  if (quotesdbP == NULL) {
    benchmark_error("Quotes database is not open");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  key.data = (char *)symbol;
  key.size = (u_int32_t) strlen(symbol) + 1;
  rc = quotesdbP->cursor(quotesdbP, txnP, &cursorp, DB_READ_COMMITTED);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Failed to create cursor for Quotes.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* Position the cursor */
  rc = cursorp->get(cursorp, &key, &data, DB_SET | DB_READ_COMMITTED | flags );
  if (rc == 0) {
    goto done;
  }
  envP->err(envP, rc, "[%s:%d] [%d] Failed to find record in Quotes.", __FILE__, __LINE__, getpid());

failXit:
  if (cursorp) {
    rc = cursorp->close(cursorp);
    if (rc != 0) {
      envP->err(envP, rc, "[%s:%d] [%d] Failed to close cursor for Quotes.", __FILE__, __LINE__, getpid());
    }
    cursorp = NULL;
  }
  if (cursorPP != NULL) {
    *cursorPP = NULL;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  return BENCHMARK_FAIL;

done:
  if (cursorPP != NULL) {
    *cursorPP = cursorp;
  }

  if (key_ret != NULL) {
    *key_ret = key;
  }

  quoteP = data.data;
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "PID: %d, retrieved: %s $%f", getpid(), quoteP->symbol, quoteP->current_price);
  if (data_ret != NULL) {
    *data_ret = data;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  return BENCHMARK_SUCCESS;
}

static int
symbol_exists(const char *symbol, DB_TXN *txnP, BENCHMARK_DBS *benchmarkP) 
{
  DBC *cursorp = NULL;
  DB  *stocksdbP = NULL;
  DB_ENV  *envP = NULL;
  DBT key, data;
  int exists = 0;
  int ret = 0;

  if (symbol == NULL || symbol[0] == '\0' || txnP == NULL || benchmarkP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  stocksdbP = benchmarkP->stocks_dbp;
  if (stocksdbP == NULL) {
    benchmark_error("Stocks database is not open");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  key.data = (char *)symbol;
  key.size = (u_int32_t) strlen(symbol) + 1;
  ret = stocksdbP->cursor(stocksdbP, txnP, &cursorp, DB_READ_COMMITTED);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to create cursor for Stocks.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* Position the cursor */
  ret = cursorp->get(cursorp, &key, &data, DB_SET);
  if (ret == 0) {
    exists = 1;
  }

  goto cleanup;

failXit:
cleanup:
  if (cursorp != NULL) {
      cursorp->close(cursorp);
  }

  return exists;
}

static int
account_exists(const char *account_id, DB_TXN *txnP, BENCHMARK_DBS *benchmarkP) 
{
  DBC *cursorp = NULL;
  DB  *personaldbP = NULL;
  DB_ENV  *envP = NULL;
  DBT key, data;
  int exists = 0;
  int ret = 0;

  if (account_id == NULL || account_id[0] == '\0' || txnP == NULL || benchmarkP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  personaldbP = benchmarkP->personal_dbp;
  if (personaldbP == NULL) {
    benchmark_error("Personal database is not open");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  key.data = (char *)account_id;
  key.size = (u_int32_t) strlen(account_id) + 1;
  ret = personaldbP->cursor(personaldbP, txnP, &cursorp, DB_READ_COMMITTED);
  if (ret != 0) {
    envP->err(envP, ret, "[%s:%d] [%d] Failed to create cursor for Personal.", __FILE__, __LINE__, getpid());
    goto failXit;
  }

  /* Position the cursor */
  ret = cursorp->get(cursorp, &key, &data, DB_SET);
  if (ret == 0) {
    exists = 1;
  }

  goto cleanup;

failXit:
cleanup:
  if (cursorp != NULL) {
      cursorp->close(cursorp);
  }

  return exists;
}

static int 
create_portfolio(const char *account_id, 
                 const char *symbol, 
                 float price, 
                 int amount, 
                 int force_apply, 
                 DB_TXN *txnP, 
                 BENCHMARK_DBS *benchmarkP)
{
  int rc = 0;
  PORTFOLIOS portfolio;
  DB_ENV  *envP = NULL;
  DBT key, data;
  int use_portfolio_id;

  envP = benchmarkP->envP;
  if (envP == NULL) {
    benchmark_error("Invalid arguments");
    goto failXit;
  }

  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  use_portfolio_id = __sync_fetch_and_add(&benchmarkP->number_portfolios, 1);

  memset(&portfolio, 0, sizeof(PORTFOLIOS));
  sprintf(portfolio.portfolio_id, "%d", use_portfolio_id);
  sprintf(portfolio.account_id, "%s", account_id);
  sprintf(portfolio.symbol, "%s", symbol);
  portfolio.to_sell = 0;
  portfolio.number_sell = 0;
  portfolio.price_sell = 0;
  if (force_apply) {
    portfolio.hold_stocks = amount;
    portfolio.to_buy = 0;
    portfolio.number_buy = 0;
    portfolio.price_buy = 0;
  }
  else {
    portfolio.hold_stocks = 0;
    portfolio.to_buy = 1;
    portfolio.number_buy = amount;
    portfolio.price_buy = price;
  }

  /* Set up the database record's key */
  key.data = portfolio.portfolio_id;
  key.size = (u_int32_t)strlen(portfolio.portfolio_id) + 1;

  /* Set up the database record's data */
  data.data = &portfolio;
  data.size = sizeof(PORTFOLIOS);

  /* Put the data into the database */
  benchmark_debug(BENCHMARK_DEBUG_LEVEL_XACT, "Inserting: [portfolio_id = %s]", (char *)key.data);

  rc = benchmarkP->portfolios_dbp->put(benchmarkP->portfolios_dbp, txnP, &key, &data, DB_NOOVERWRITE);
  if (rc != 0) {
    envP->err(envP, rc, "[%s:%d] [%d] Database put failed (id: %s).", __FILE__, __LINE__, getpid(), (char *)key.data);
    goto failXit; 
  }

  goto cleanup;

failXit:
  rc = 1;

cleanup:
  return rc;
}

int
benchmark_handle_alloc(void **benchmark_handle,
                       int create,
                       const char *program,
                       const char *homedir,
                       const char *datafilesdir)
{
  BENCHMARK_DBS *benchmarkP = NULL;
  int ret = BENCHMARK_FAIL;

  set_benchmark_debug_level(BENCHMARK_DEBUG_LEVEL_MIN);

  benchmarkP = malloc(sizeof (BENCHMARK_DBS));
  if (benchmarkP == NULL) {
    goto failXit;
  }

  initialize_benchmarkdbs(benchmarkP);
  benchmarkP->magic = BENCHMARK_MAGIC_WORD;
  if (create) benchmarkP->createDBs = 1;
  benchmarkP->db_home_dir = homedir;
  benchmarkP->datafilesdir = datafilesdir;

  /* Identify the files that hold our databases */
  set_db_filenames(benchmarkP);

  ret = databases_setup(benchmarkP, ALL_DBS_FLAG, program, stderr);
  if (ret != 0) {
    benchmark_error("Error opening databases.");
    goto failXit;
  }

  if (create) benchmarkP->createDBs = 0;

#if 0
  ret = benchmark_stocks_symbols_get(benchmarkP);
  if (ret != 0) {
    goto failXit;
  }
#endif

  BENCHMARK_CHECK_MAGIC(benchmarkP);

  *benchmark_handle = benchmarkP;

  return BENCHMARK_SUCCESS;

failXit:
  if (benchmarkP) {
    benchmark_handle_free(benchmarkP);
    benchmarkP = NULL;
  }

  *benchmark_handle = NULL;
  return BENCHMARK_FAIL;
}

int
benchmark_handle_free(void *benchmark_handle)
{
  BENCHMARK_DBS *benchmarkP = benchmark_handle;

  if (benchmarkP == NULL) {
    return BENCHMARK_SUCCESS;
  }

  BENCHMARK_CHECK_MAGIC(benchmarkP);
  if (databases_close(benchmarkP) != BENCHMARK_SUCCESS) {
    return BENCHMARK_FAIL;
  }
  if (close_environment(benchmarkP) != BENCHMARK_SUCCESS) {
    return BENCHMARK_FAIL;
  }

  free(benchmarkP->stocks_db_name);
  free(benchmarkP->quotes_db_name);
  free(benchmarkP->quotes_hist_db_name);
  free(benchmarkP->portfolios_db_name);
  free(benchmarkP->portfolios_sdb_name);
  free(benchmarkP->accounts_db_name);
  free(benchmarkP->currencies_db_name);
  free(benchmarkP->personal_db_name);

  /* Don't forget to free the list of stocks */
  if (benchmarkP->number_stocks > 0 && benchmarkP->stocks != NULL) {
    int i;
    for (i=0; i<benchmarkP->number_stocks; i++) {
      if (benchmarkP->stocks[i] != NULL) {
        free(benchmarkP->stocks[i]);
        benchmarkP->stocks[i] = NULL;
      }
    }
  }

  benchmarkP->magic = 0;
  free(benchmarkP);

  return BENCHMARK_SUCCESS;
}
