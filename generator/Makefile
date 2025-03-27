CC = cc
CFLAGS = -I/opt/homebrew/include
LDFLAGS = -L/opt/homebrew/lib
LIBS = -lgsl -lgslcblas

all: generator

generator:
	$(CC) generator.c -o generator $(CFLAGS) $(LDFLAGS) $(LIBS)

clean:
	rm generator