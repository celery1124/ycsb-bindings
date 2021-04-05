TARGET=libkvrangedbjni.so

LIB_DIR=/home/grads/c/celery1124/kvrangedb
CC=g++
INCLUDES=-I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -I$(LIB_DIR)/include
LIBS=-L$(LIB_DIR)/libs -lrt -lkvrangedb
SRC=kvrangedbjni.cc json11.cpp
CXXFLAGS=-std=c++11 -fPIC -O2

all:
		$(CC) -shared -o $(TARGET) $(SRC) $(INCLUDES) $(LIBS) $(CXXFLAGS)
