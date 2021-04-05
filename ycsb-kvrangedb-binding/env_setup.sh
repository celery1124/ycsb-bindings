#!/bin/bash

LIB_HOME="../kvrangedb"
LIB_HOME2="../wisckey"
export LD_LIBRARY_PATH=${LIB_HOME}/libs:${LIB_HOME2}/libs:$(pwd)/libs

