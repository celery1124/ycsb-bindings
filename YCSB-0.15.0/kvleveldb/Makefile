TARGET=libkvleveldbjni.so

LIB_DIR=/home/colouser05/kvs_level
CC=g++
INCLUDES=-I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -I$(LIB_DIR)/include
LIBS=-L$(LIB_DIR)/out-shared -lrt -lleveldb
SRC=kvleveldbjni.cc json11.cpp
CXXFLAGS=-std=c++11 -fPIC -O2

all:
		$(CC) -shared -o $(TARGET) $(SRC) $(INCLUDES) $(LIBS) $(CXXFLAGS)
