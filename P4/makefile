CPP = g++
CC = gcc

LIB = -lpthread -ldl
BIN = sqlite load

all : $(BIN)
sqlite : sqlite3.c shell.c
	$(CC) -o $@ $^ $(LIB)
load : load.cpp sqlite3.o
	$(CPP) -o $@ $^ $(LIB)
sqlite3.o : sqlite3.c
	$(CC) -o $@ -c $^

clean :
	rm -f $(BIN) *.o

.PHONY: all, clean

