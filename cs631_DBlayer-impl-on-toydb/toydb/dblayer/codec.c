#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "codec.h"

typedef union {
    int i;
    byte  bytes[4];
} IntBytes;

int
EncodeInt(int i, byte *bytes) {
    IntBytes ib;
    ib.i = i;
    memcpy(bytes, ib.bytes, 4);
    return 4;
}

int
DecodeInt(byte *bytes) {
    IntBytes ib;
    memcpy(ib.bytes, bytes, 4);
    return ib.i;
}

typedef union {
    short s;
    byte  bytes[2];
} ShortBytes;

/*
  Copies an encoding of a 16-bit short to a buf that must contain at least 2 bytes.
 */
int
EncodeShort(short s, byte *bytes) {
    ShortBytes sb;
    sb.s = s;
    memcpy(bytes, sb.bytes, 2);
    return 2;
}

/*
 Translates 2 bytes (pointed to by bytes) to a short.
 */
short
DecodeShort(byte *bytes) {
    ShortBytes sb;
    memcpy(sb.bytes, bytes, 2);
    return sb.s;
}

typedef union {
    long long ll;
    byte   bytes[8];
} LongBytes;

/*
 Naive encoding of a 64-bit long to 8 bytes. 
 */
int
EncodeLong(long long l, byte *bytes) {
    LongBytes lb;
    lb.ll = l;
    memcpy(bytes, lb.bytes, 8);
    return 8;
}

/*
  Translates 8 bytes pointed to by 'bytes' to a 64-bit long.
 */
long long
DecodeLong(byte *bytes) {
    LongBytes lb;
    memcpy(lb.bytes, bytes, 8);
    return lb.ll;
}

/*
  Copies a null-terminated string to a <len><bytes> format. The length 
  is encoded as a 2-byte short. 
  Precondition: the 'bytes' buffer must have max_len bytes free.
  Returns the total number of bytes encoded (including the length)
 */
int
EncodeCString(char *str, byte *bytes, int max_len) {
    int len = strlen(str);
    if (len + 2 > max_len) {
	len = max_len - 2;
    }
    EncodeShort((short)len, bytes);
    memcpy(bytes+2, str, len);
    return len+2;
}

int
DecodeCString(byte *bytes, char *str, int max_len) {
    int len = DecodeShort(bytes);
    if (len + 1 > max_len) { // account for null terminator.
	len = max_len - 1; 
    }
    memcpy(str, bytes+2, len);
    str[len] = '\0';
    return len;
}


int
stricmp(char const *a, char const *b)
{
    for (;; a++, b++) {
        int d = tolower((unsigned char)*a) - tolower((unsigned char)*b);
        if (d != 0 || !*a)
            return d;
    }
}

/*
int
main() {
    char *test = "HelloWorld";

    char buf[8];
    EncodeLong(123443211234, buf);

    long long l = DecodeLong(buf);
    printf("%lld \n", l);

    EncodeShort((short)strlen(test), buf);
    printf ("%d\n", DecodeShort(buf));

    char sbuf[100];
    int len = EncodeCString(test, sbuf, sizeof(sbuf));

    printf ("Encoded %d bytes\n", len);
    
    char str[20];
    DecodeCString(sbuf, str, sizeof(sbuf));
    printf ("'%s'\n", str);
}
*/
