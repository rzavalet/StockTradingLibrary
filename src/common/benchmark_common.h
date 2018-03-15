#ifndef _BENCHMARK_COMMON_H_
#define _BENCHMARK_COMMON_H_
/*
 * =====================================================================================
 *
 *       Filename:  benchmark_common.h
 *
 *    Description:  This file contains the definition of the tables that will be
 *                  used by the bechmarking application
 *
 *        Version:  1.0
 *        Created:  08/14/16 18:19:05
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ricardo Zavaleta (rj.zavaleta@gmail.com)
 *   Organization:  CINVESTAV
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <db.h>

#define BENCHMARK_NUM_SYMBOLS  (10)
#define BENCHMARK_NUM_ACCOUNTS (50)

#define BENCHMARK_SUCCESS   (0)
#define BENCHMARK_FAIL      (1)

#define BENCHMARK_MAGIC_WORD   (0xCAFE)
#define CHRONOS_SHMKEY 35

#define BENCHMARK_CHECK_MAGIC(_benchmarkP)   assert((_benchmarkP)->magic == BENCHMARK_MAGIC_WORD)

/* TODO: Are these constants useful? */
#define   ID_SZ           10
#define   NAME_SZ         128
#define   PWD_SZ          32
#define   USR_SZ          32
#define   LONG_NAME_SZ    200
#define   PHONE_SZ        16


#define MAXLINE   1024

#define PRIMARY_DB	0
#define SECONDARY_DB	1

/*
 * This benchmark is based on Kyoung-Don Kang et al. 
 * See the paper for reference.
 **/

/* These are the tables names in our app */
#define STOCKSDB          "Stocks"
#define QUOTESDB          "Quotes"
#define QUOTES_HISTDB     "Quotes_Hist"
#define PORTFOLIOSDB      "Portfolios"
#define PORTFOLIOSSECDB   "PortfoliosSec"
#define ACCOUNTSDB        "Accounts"
#define CURRENCIESDB      "Currencies"
#define PERSONALDB        "Personal"

/* Some of the tables above are catalogs. We have static
 * files that we use to populate them */
#define PERSONAL_FILE     "accounts.txt"
#define STOCKS_FILE       "companylist.txt"
#define CURRENCIES_FILE   "currencies.txt"
#define QUOTES_FILE       "quotes.txt"

#define STOCKS_FLAG       0x0001
#define PERSONAL_FLAG     0x0002
#define CURRENCIES_FLAG   0x0004
#define QUOTES_FLAG       0x0008
#define QUOTES_HIST_FLAG  0x0010
#define PORTFOLIOS_FLAG   0x0020
#define ACCOUNTS_FLAG     0x0040
#define ALL_DBS_FLAG      0x00FF

#define IS_STOCKS(_v)       (((_v) & STOCKS_FLAG) == STOCKS_FLAG)
#define IS_PERSONAL(_v)     (((_v) & PERSONAL_FLAG) == PERSONAL_FLAG)
#define IS_CURRENCIES(_v)   (((_v) & CURRENCIES_FLAG) == CURRENCIES_FLAG)
#define IS_QUOTES(_v)       (((_v) & QUOTES_FLAG) == QUOTES_FLAG)
#define IS_QUOTES_HIST(_v)  (((_v) & QUOTES_HIST_FLAG) == QUOTES_HIST_FLAG)
#define IS_PORTFOLIOS(_v)   (((_v) & PORTFOLIOS_FLAG) == PORTFOLIOS_FLAG)
#define IS_ACCOUNTS(_v)     (((_v) & ACCOUNTS_FLAG) == ACCOUNTS_FLAG)


typedef struct benchmark_xact_data_t {
  char      accountId[ID_SZ];
  int       symbolId;
  char      symbol[ID_SZ];
  float     price;
  int       amount;
} benchmark_xact_data_t;

typedef struct benchmark_data_packet_t {
  size_t   size;
  size_t   used;
  benchmark_xact_data_t *data;
} benchmark_data_packet_t;

typedef void *benchmark_xact_h;

/* Let's define our Benchmark DB, which translates to
 * multiple berkeley DBs*/
typedef struct benchmark_dbs {

  int magic;
  int createDBs;

  /* This is the environment */
  DB_ENV  *envP;

  /* These are the dbs */
  DB  *stocks_dbp;
  DB  *quotes_dbp;
  DB  *quotes_hist_dbp;
  DB  *portfolios_dbp;
  DB  *accounts_dbp;
  DB  *currencies_dbp;
  DB  *personal_dbp;

  /* secondary databases */
  DB  *portfolios_sdbp;

  /* Some other useful information */
  const char *db_home_dir;
  const char *datafilesdir;
  
  /* Primary databases */
  char *stocks_db_name;
  char *quotes_db_name;
  char *quotes_hist_db_name;
  char *portfolios_db_name;
  char *accounts_db_name;
  char *currencies_db_name;
  char *personal_db_name;

  /* secondary databases */
  char *portfolios_sdb_name;

  /* How many stores do we have in the system */
  int   number_stocks;
  char **stocks;

  int   number_portfolios;

} BENCHMARK_DBS;

#define BENCHMARK_STOCKS_LIST(_benchmarkP)  (((BENCHMARK_DBS *)_benchmarkP)->stocks)
#define BENCHMARK_NUM_STOCKS(_benchmarkP)    (((BENCHMARK_DBS *)_benchmarkP)->number_stocks)
#define BENCHMARK_SET_CREATE_DB(_benchmarkP)  (((BENCHMARK_DBS *)_benchmarkP)->createDBs = 1)
#define BENCHMARK_CLEAR_CREATE_DB(_benchmarkP)  (((BENCHMARK_DBS *)_benchmarkP)->createDBs = 0)

/* Now let's define the structures for our tables */
/* TODO: Can we improve cache usage by modifying sizes? */

typedef struct stock {
  char              stock_symbol[ID_SZ];
  char              full_name[NAME_SZ];
} STOCK;

typedef struct quote {
  char      symbol[ID_SZ];
  float     current_price;
  char      trade_time[ID_SZ];
  float     low_price_day;
  float     high_price_day;
  float     perc_price_change;
  float     bidding_price;
  float     asking_price;
  long      trade_volume;
  char      market_cap[ID_SZ];
} QUOTE;

typedef struct quotes_hist {
  char      symbol[ID_SZ];
  int       current_price;
  time_t    trade_time;
  int       low_price_day;
  int       high_price_day;
  int       perc_price_change;
  int       bidding_price;
  int       asking_price;
  int       trade_volume;
  int       market_cap;
} QUOTES_HIST;

typedef struct portfolios {
  char      portfolio_id[ID_SZ];
  char      account_id[ID_SZ];
  char      symbol[ID_SZ];
  int       hold_stocks;
  char      to_sell;
  int       number_sell;
  int       price_sell;
  char      to_buy;
  int       number_buy;
  int       price_buy;
} PORTFOLIOS;

typedef struct account {
  char      account_id[ID_SZ];
  char      user_name[USR_SZ];
  char      password[PWD_SZ];
} ACCOUNT;

typedef struct currency {
  char      currency_symbol[ID_SZ];
  char      country[LONG_NAME_SZ];
  char      currency_name[NAME_SZ];
  int       exchange_rate_usd;
} CURRENCY;

typedef struct personal {
  char      account_id[ID_SZ];
  char      last_name[NAME_SZ];
  char      first_name[NAME_SZ];
  char      address[NAME_SZ];
  char      address_2[NAME_SZ];
  char      city[NAME_SZ];
  char      state[NAME_SZ];
  char      country[NAME_SZ];
  char      phone[PHONE_SZ];
  char      email[LONG_NAME_SZ];
} PERSONAL;


/* Function prototypes */
int	databases_setup(BENCHMARK_DBS *, int, const char *, FILE *);
int	databases_open(DB **, const char *, const char *, FILE *, int);
int	databases_close(BENCHMARK_DBS *);
int close_environment(BENCHMARK_DBS *benchmarkP);

void	initialize_benchmarkdbs(BENCHMARK_DBS *);
void	set_db_filenames(BENCHMARK_DBS *my_stock);

int 
show_portfolios(char              *account_id, 
                int               showOnlyUsers, 
                benchmark_xact_h  xactH,
                BENCHMARK_DBS     *benchmarkP);

int 
show_all_portfolios(BENCHMARK_DBS *benchmarkP);

int 
place_order(const char *account_id, 
            const char *symbol, 
            float price, 
            int amount, 
            int force_apply, 
            benchmark_xact_h  xactH,
            BENCHMARK_DBS *benchmarkP);

int 
update_stock(char *symbolP, float newValue, BENCHMARK_DBS *benchmarkP);

int 
sell_stocks(const char *account_id, 
            const char *symbol, 
            float price, 
            int amount, 
            int force_apply, 
            benchmark_xact_h  xactH,
            BENCHMARK_DBS *benchmarkP);

int 
show_stocks_records(char *symbolId, BENCHMARK_DBS *benchmarkP);

int
show_quote(char *symbolP, benchmark_xact_h xactH, BENCHMARK_DBS *benchmarkP);

int 
show_currencies_records(BENCHMARK_DBS *my_benchmarkP);

int
show_one_portfolio(char *account_id, DB_TXN  *txn_inP, BENCHMARK_DBS *benchmarkP);

int
show_personal_item(void *vBuf);

int
show_portfolio_item(void *vBuf, char **symbolIdPP);

int 
start_xact(benchmark_xact_h *xact_ret, const char *txn_name, BENCHMARK_DBS *benchmarkP);

int 
abort_xact(benchmark_xact_h xactH, BENCHMARK_DBS *benchmarkP);

int 
commit_xact(benchmark_xact_h xactH, BENCHMARK_DBS *benchmarkP);

int
benchmark_handle_free(void *benchmark_handle);

int
benchmark_handle_alloc(void **benchmark_handle,
                       int create,
                       const char *program,
                       const char *homedir,
                       const char *datafilesdir);
/*---------------------------------
 * Debugging routines
 *-------------------------------*/
#define BENCHMARK_DEBUG_LEVEL_MIN   (0)
#define BENCHMARK_DEBUG_LEVEL_MAX   (10)
#define BENCHMARK_DEBUG_LEVEL_OP    (6)
#define BENCHMARK_DEBUG_LEVEL_XACT  (5)
#define BENCHMARK_DEBUG_LEVEL_API   (4)

int benchmark_debug_level;

#define set_benchmark_debug_level(_level)  \
  (benchmark_debug_level = (_level))

#define benchmark_debug(level,...) \
  do {                                                         \
    if (benchmark_debug_level >= level) {                        \
      char _local_buf_[256];                                   \
      snprintf(_local_buf_, sizeof(_local_buf_), __VA_ARGS__); \
      fprintf(stderr, "DEBUG: %s:%d: %s", __FILE__, __LINE__, _local_buf_);    \
      fprintf(stderr, "\n");	   \
    } \
  } while(0)



/*---------------------------------
 * Error routines
 *-------------------------------*/
#define benchmark_msg(_prefix, _fp, ...) \
  do {                     \
    char _local_buf_[256];				     \
    snprintf(_local_buf_, sizeof(_local_buf_), __VA_ARGS__); \
    fprintf(_fp, "%s: %s: at %s:%d", _prefix,_local_buf_, __FILE__, __LINE__);   \
    fprintf(_fp,"\n");					     \
  } while(0)

#if BENCHMARK_DEBUG
#define benchmark_info(...) \
  benchmark_msg("INFO", stderr, __VA_ARGS__)
#else
#define benchmark_info(...)
#endif

#define benchmark_error(...) \
  benchmark_msg("ERROR", stderr, __VA_ARGS__)

#define benchmark_warning(...) \
  benchmark_msg("WARN", stderr, __VA_ARGS__)
#endif
