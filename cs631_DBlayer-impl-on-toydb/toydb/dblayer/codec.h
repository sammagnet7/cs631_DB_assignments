#include "tbl.h"

int
EncodeInt(int i, byte *bytes);
int
DecodeInt(byte *bytes);

int
EncodeShort(short s, byte *bytes) ;
short
DecodeShort(byte *bytes);

int
EncodeLong(long long l, byte *bytes);

long long
DecodeLong(byte *bytes);

int
EncodeCString(char *str, byte *bytes, int max_len);

int
DecodeCString(byte *bytes, char *str, int max_len);
