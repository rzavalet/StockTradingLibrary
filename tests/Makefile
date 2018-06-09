ROOT=../../
CC=gcc
CFLAGS= -I/home/ricardozavaleta/usr/include -I/usr/local/BerkeleyDB.6.2/include -L/home/ricardozavaleta/usr/lib -L/usr/local/BerkeleyDB.6.2/lib -g -Wall
LIBS=-lstocktrading -ldb-6.2

EXE = test1 test2
OBJ = $(patsubst %,%.o,$(EXE))

all: $(EXE)

$(OBJ): %.o: %.c
	$(CC) -c $(CFLAGS) -o $@ $<

$(EXE): %: %.o
	$(CC) -o $@ $< $(CFLAGS) $(LIBS)

init :
	-@echo "#---------------------------------------------------#"
	-@echo "#--------- Setting up directory structure ----------#"
	-@echo "#---------------------------------------------------#"
	-rm -rf /tmp/cnt*
	-rm -rf /tmp/upd*
	-rm -rf /tmp/chronos/databases
	-rm -rf /tmp/chronos/datafiles
	mkdir -p /tmp/chronos/databases
	mkdir -p /tmp/chronos/datafiles
	cp ../datafiles/* /tmp/chronos/datafiles
	-@echo "#-------------------- DONE -------------------------#"

cscope:
	cscope -bqRv

.PHONY: clean

clean:
	rm -rf $(EXE)
	rm -rf $(OBJ)
