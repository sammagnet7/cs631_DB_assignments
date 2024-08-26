#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>

#define VARCHAR 1
#define INT     2
#define LONG    3

typedef char byte;

typedef struct {
    char *name;
    int  type;  // one of VARCHAR, INT, LONG
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc **columns; // array of column descriptors
} Schema;

typedef struct {
    short numRecords;
    short freespaceoffset; // Stores the offset to the end of freespace region in the pagebuf
    short recordoffset[]; // Stores the offset of each record in pagebuf which are stored bottom up
} PageHeader;

typedef struct {
    Schema *schema;

    int file_descriptor; // Cache the file descriptor associated with the file representing the table
    int firstPageNum; // Store the address of the head of the page list of the file
    char* pagebuf; // Points to a page's data buffer, initialised with the first page's buffer
} Table ;

typedef int RecId;

int
Table_Open(char *fname, Schema *schema, bool overwrite, Table **table);

int
Table_Insert(Table *t, byte *record, int len, RecId *rid);

int
Table_Get(Table *t, RecId rid, byte *record, int maxlen);

void
Table_Close(Table *);

typedef void (*ReadFunc)(void *callbackObj, RecId rid, byte *row, int len);

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn);

#endif
