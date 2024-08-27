#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>

#define VARCHAR 1
#define INT     2
#define LONG    3

#define ENTRY_SIZE sizeof(short)
#define PAGE_HEADER(pagebuffer) ((PageHeader*)pagebuffer)
#define HEADER_SIZE(header) ((header->numRecords + 2/*For recordnum and freespaceoffset*/) * sizeof(short)) 

#define NUM_HEADER_ENTRIES(header) (header->numRecords + 2)

#define HEADER_SIZE(header) (NUM_HEADER_ENTRIES(header) * ENTRY_SIZE)
#define FREESPACE_SIZE(header) (header->freespaceoffset - (HEADER_SIZE(header) - 1))
#define FREESPACE_REGION(header, pagebuffer, length) (pagebuffer+header->freespaceoffset - length + 1)
#define RECORD_OFFSET_ARRAY_SIZE(header) (HEADER_SIZE(header) - 2*ENTRY_SIZE)
#define NEXT_SLOT(header) (RECORD_OFFSET_ARRAY_SIZE(header)/ENTRY_SIZE)

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

typedef struct DirtyPageNode {
    int pageNum;                 // Page number of the dirty page
    struct DirtyPageNode *next;  // Pointer to the next node in the linked list
} DirtyPageNode;


typedef struct {
    Schema *schema;

    int file_descriptor; // Cache the file descriptor associated with the file representing the table
    int firstPageNum; // Store the address of the head of the page list of the file
    int currentPageNum; // Store the address of the current page of the table being referred
    char* pagebuf; // Points to a page's data buffer, initialised with the first page's buffer
    DirtyPageNode* dirtyPageList; // To track the pages made dirty while Table was used.
    int dirtyListSize;
} Table ;

// Helpers
int
Alloc_NewPage(Table* table);

int
Find_FreeSpace(Table* table, int len);

int 
Get_FreeSpaceLen(Table* table);

int
Copy_ToFreeSpace(Table* table, byte* record, int len, RecId** rid);

int
Init_DirtyList(Table *table);
int
MarkPage_Dirty(Table *table, int pagenum);

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
