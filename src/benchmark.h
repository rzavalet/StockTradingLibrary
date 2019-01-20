#ifndef _BENCHMARK_H_
#define _BENCHMARK_H_

typedef void *BENCHMARK_H;
typedef void *BENCHMARK_DATA_PACKET_H;

int 
benchmark_handle_alloc(BENCHMARK_H *benchmark_handle,
                       int create, 
                       const char *program, 
                       const char *homedir, 
                       const char *datafilesdir);

int 
benchmark_handle_free(BENCHMARK_H benchmark_handle);

int 
benchmark_initial_load(const char *program,
                       const char *homedir, 
                       const char *datafilesdir);

int
benchmark_load_portfolio(BENCHMARK_H benchmark_handle);

int
benchmark_portfolios_stats_get(BENCHMARK_H benchmark_handle);

int
benchmark_refresh_quotes(BENCHMARK_H benchmark_handle, 
                         int *symbolP, 
                         float newValue);

int
benchmark_refresh_quotes2(BENCHMARK_H benchmark_handle, 
                          const char *symbolP, 
                          float newValue);

int
benchmark_refresh_quotes_list(int            num_symbols,
                              const char   **symbols_list,
                              float         *prices_list,
                              void          *benchmark_handle);
int
benchmark_view_stock(BENCHMARK_H benchmark_handle, 
                     int *symbolP);

int
benchmark_view_stock2(int num_symbols, 
                      const char **symbol_list_P, 
                      BENCHMARK_H benchmark_handle);

int
benchmark_view_portfolio(BENCHMARK_H  benchmark_handle);

int
benchmark_purchase2(BENCHMARK_DATA_PACKET_H data_packetH,
                    BENCHMARK_H           benchmark_handle);
int
benchmark_view_portfolio2(int           num_accounts, 
                          const char    **account_list_P, 
                          BENCHMARK_H   benchmark_handle);
int
benchmark_purchase(int      account, 
                   int      symbol, 
                   float    price, 
                   int      amount, 
                   int      force_apply, 
                   BENCHMARK_H  benchmark_handle, 
                   int      *symbolP);

int
benchmark_sell(int    account, 
               int    symbol, 
               float  price, 
               int    amount, 
               int    force_apply, 
               BENCHMARK_H benchmark_handle, 
               int    *symbol_ret);

int
benchmark_sell2(BENCHMARK_DATA_PACKET_H data_packetH,
                void *benchmark_handle);

int
benchmark_stock_list_get(BENCHMARK_H benchmark_handle, 
                         char ***stocks_list, 
                         int *num_stocks);

int
benchmark_stock_list_from_file_get(const char      *homedir,
                                   const char      *datafilesdir,
                                   unsigned int     num_stocks,
                                   char          ***stock_list_ret);

int
benchmark_data_packet_alloc(size_t reqsz, 
                            BENCHMARK_DATA_PACKET_H *data_packetH);

int
benchmark_data_packet_free(BENCHMARK_DATA_PACKET_H data_packetH);

int
benchmark_data_packet_append(const char  *accountId,
                             int          symbolId,
                             const char  *symbol,
                             float        price,
                             int          amount,
                             BENCHMARK_DATA_PACKET_H data_packetH);

#endif
