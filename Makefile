CC=gcc
CFLAGS=-Wall -g -O
OBJ=prefetchd.o bitarray.o cache-sim.o
PROG=prefetchd
LDFLAGS+=-lrt -lm 
#-lmcheck

all: $(PROG)

.o: %.c
	$(CC) -c $(CFLAGS) -o $@ $<

cache.o: cache.cc
	$(CXX) -c $(CFLAGS) -o $@ $<


$(PROG): $(OBJ)
	$(CC) $(LDFLAGS) -o $@ $^

clean:
	$(RM) $(PROG) $(OBJ)
