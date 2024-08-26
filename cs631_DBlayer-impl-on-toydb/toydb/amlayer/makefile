
CC=cc
CFLAGS = -g

OBJS=am.o amfns.o amsearch.o aminsert.o amstack.o amglobals.o amscan.o amprint.o misc.o

a.out : $(OBJS) ../pflayer/pflayer.a main.o amlayer.a
	$(CC) $(CFLAGS) main.o amlayer.a ../pflayer/pflayer.a

amlayer.a: $(OBJS)
	ld -r $(OBJS) -o amlayer.a

am.o : am.c am.h pf.h
	$(CC) $(CFLAGS) -c am.c

amfns.o : amfns.c am.h pf.h
	$(CC) $(CFLAGS) -c amfns.c

amsearch.o : amsearch.c am.h pf.h
	$(CC) $(CFLAGS) -c amsearch.c

aminsert.o : aminsert.c am.h pf.h
	$(CC) $(CFLAGS) -c aminsert.c

amscan.o : amscan.c am.h pf.h
	$(CC) $(CFLAGS) -c amscan.c

amstack.o : amstack.c am.h pf.h
	$(CC) $(CFLAGS) -c amstack.c

amglobals.o : amglobals.c am.h
	$(CC) $(CFLAGS) -c amglobals.c

amprint.o : amprint.c am.h pf.h 
	$(CC) $(CFLAGS) -c amprint.c

main.o : main.c am.h pf.h 
	$(CC) $(CFLAGS) -c main.c


clean:
	rm  -f *.o *.a a.out *~
