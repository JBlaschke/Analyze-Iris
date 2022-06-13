CC   ?= cc
CXX  ?= CC
NVCC ?= nvcc


CFLAGS    ?= -fPIC -O3 -funroll-loops -march=native -g -rdynamic
CXXFLAGS  ?= $(CFLAGS) -std=c++14


%.o: %.cpp
	$(CXX) -c $(CXXFLAGS) $< -o $@
%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@
%:%.o
	$(CC) $(CFLAGS) -o $@ $^

default: all

all: patient_app

clean:
	rm patient_app
