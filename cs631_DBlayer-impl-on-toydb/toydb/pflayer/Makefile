# Uncomment to enable 'American Fuzzy Lop' for fuzz testing
#CC=afl-clang

CC=cc
CFLAGS = -g
#PUBLICDIR= /usr0/cs564/public/project
SRC= buf.c hash.c pf.c
OBJ= buf.o hash.o pf.o
HDR = pftypes.h pf.h 

pflayer.a: $(OBJ)
	ld -r -o pflayer.a $(OBJ)

tests: testhash testpf

testpf: testpf.o pflayer.a
	$(CC) $(CFLAGS) -o testpf testpf.o pflayer.a

testhash: testhash.o pflayer.a
	$(CC) $(CFLAGS) -o testhash testhash.o pflayer.a

$(OBJ): $(HDR)

testhash.o: $(HDR)

testpf.o: $(HDR)

lint: 
	lint $(SRC)

install: pflayer.a 

clean:
	rm -f *.out *.o *.a *~ test1 test2 testhash testpf
