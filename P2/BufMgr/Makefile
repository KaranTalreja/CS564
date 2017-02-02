##############################################################
#               CMake Project Wrapper Makefile               #
############################################################## 

all:
	cd src;\
	g++ -g -gdwarf-2 -std=c++11 *.cpp exceptions/*.cpp -I. -Wall -o badgerdb_main

clean:
	cd src;\
	rm -f badgerdb_main test.? ../test.?

doc:
	doxygen Doxyfile
