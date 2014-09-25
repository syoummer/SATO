UNAME		= $(shell uname)
OPTFLAGS	= -O2 -Wall -std=c++0x
INCBASE		= -I. -I../../
LIBBASE		= -lspatialindex -lboost_program_options

INCFLAGS = -I ${SATO_INC_PATH}
LIBS = -L ${SATO_LIB_PATH}

CFLAGS      = $(OPTFLAGS) $(INCBASE) $(INCFLAGS) 
LDFLAGS     = $(LIBBASE) $(LIBS)

ifeq ($(UNAME), Darwin)
    CC = gcc -arch x86_64
    CXX = g++ -arch x86_64
else 
    CC = gcc
    CXX = g++44
endif

TARGET = hc

all: $(TARGET)

debug: CXX += -DDEBUG -g
debug: CC += -DDEBUG -g
debug: all 

hc: serialhc.cc
	$(CXX) $< -std=c++0x $(CFLAGS) $(LDFLAGS) -o $@

clean:
	@rm -f $(TARGET)

