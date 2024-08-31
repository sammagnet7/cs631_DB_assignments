#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>

#define VARCHAR 1
#define INT     2
#define LONG    3

#define HEADER_FIELD_SIZE sizeof(short)
#define PAGE_HEADER(pagebuffer) ((PageHeader*)pagebuffer)

#define NUM_HEADER_FIELD(header) (header->numRecords + 2) // count of recordoffset array + two other fields

#define HEADER_SIZE(header) (NUM_HEADER_FIELD(header) * HEADER_FIELD_SIZE)
#define FREESPACE_SIZE(header) (header->freespaceoffset - (HEADER_SIZE(header) - 1))
#define INPAGE_MAX_RECORD_SPACE (PF_PAGE_SIZE - 3*HEADER_FIELD_SIZE) // Only three entry in header
#define FREESPACE_INSERTION_INDEX(header,pagebuffer,length) (pagebuffer+header->freespaceoffset - length + 1) // Calculates the address where a new record is inserted
#define RECORD_OFFSET_ARRAY_SIZE(header) (HEADER_SIZE(header) - 2*HEADER_FIELD_SIZE)
#define GET_OFFSET_AT_SLOT(header,slot) (header->recordoffset[slot])
#define NEXT_SLOT(header) (RECORD_OFFSET_ARRAY_SIZE(header)/HEADER_FIELD_SIZE)
#define BUILD_RECORD_ID(pagenum,slot) (pagenum << 16 | slot)
#define RECORD_SIZE_AT_SLOT(header,slot) (slot == 0)?(PF_PAGE_SIZE - header->recordoffset[0]):(header->recordoffset[slot-1] - header->recordoffset[slot]);
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
    short numRecords;      // Stores the number of records in the page
    short freespaceoffset; // Stores the offset to the end of freespace region in the pagebuf
    short recordoffset[]; // Stores the offset of each record in pagebuf which are stored bottom up
} PageHeader;


typedef struct {
    Schema *schema;

    int file_descriptor; // Cache the file descriptor associated with the file representing the table
    int firstPageNum; // Store the address of the head of the page list of the file
    int currentPageNum; // Store the address of the current page of the table being referred
    char* pagebuf; // Points to a page's data buffer
} Table ;

typedef int RecId;

// Helpers
int
Alloc_NewPage(Table* table);

int
Find_FreeSpace(Table* table, int len);

int
Copy_ToFreeSpace(Table* table, byte* record, int len, RecId* rid);



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
