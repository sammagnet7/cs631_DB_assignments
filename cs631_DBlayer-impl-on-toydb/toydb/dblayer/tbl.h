#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>

#define VARCHAR 1
#define INT     2
#define LONG    3

// IMPLEMENTED---------------------------------------------------------------------------------------

#define PAGEHEADER_FIXED_ATTR_COUNT (2)
#define PAGEHEADER_VARIABLE_ATTR_COUNT (header->numRecords)
#define PAGEHEADER_ATTR_SIZE (sizeof(short))
#define PAGEHEADER_SIZE(header) ( (PAGEHEADER_VARIABLE_ATTR_COUNT + PAGEHEADER_FIXED_ATTR_COUNT) *PAGEHEADER_ATTR_SIZE) // #recordoffset + numRecords + freespaceoffset

#define INPAGE_FREESPACE_LEFT(header) (header->freespaceoffset - PAGEHEADER_SIZE(header) + 1) 
#define INPAGE_MAXPOSS_RECORD_SIZE (PF_PAGE_SIZE - 3*PAGEHEADER_ATTR_SIZE) // Three attributes in page header
#define INPAGE_INSERT_REGION(header,pagebuffer,length) ( (pagebuffer + header->freespaceoffset) - length + 1) 

#define BUILD_RECORD_ID(pagenum,slot) (pagenum << 16 | slot) // 4 Byte Rec ID : [ (MSB) 2 Byte Page num | 2 Byte slot num inside that page (LSB) ]
#define INSLOT_RECORD_SIZE(header,slot) ( (slot == 0) ?\
                                                (PF_PAGE_SIZE - header->recordoffset[0])\ 
                                                    : (header->recordoffset[slot-1] - header->recordoffset[slot]) ); 
// ---------------------------------------------------------------------------------------

typedef char byte;

typedef struct {
    char *name;
    int  type;  // one of VARCHAR, INT, LONG
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc **columns; // array of column descriptors
} Schema;

// IMPLEMENTED---------------------------------------------------------------------------------------
typedef struct {
    short numRecords;      // Stores the number of records in the page
    short freespaceoffset; // Stores the offset to the end of freespace region in the pagebuf
    short recordoffset[]; // Stores the offset of each record in pagebuf which are stored bottom up
} PageHeader;

typedef struct DirtyPageNode {
    int pageNum;                 // Page number of the dirty page
    struct DirtyPageNode *next;  // Pointer to the next node in the linked list
} DirtyPageNode;

// ---------------------------------------------------------------------------------------


typedef struct {
    Schema *schema;
// IMPLEMENTED---------------------------------------------------------------------------------------

    int file_descriptor; // Cache the file descriptor associated with the file representing the table
    int firstPageNum; // Store the address of the head of the page list of the file
    int currentPageNum; // Store the address of the current page of the table being referred
    char* pagebuf; // Points to a page's data buffer, initialised with the first page's buffer
    DirtyPageNode* dirtyPageList; // To track the pages made dirty while Table was used.
    int dirtyListSize;
// ---------------------------------------------------------------------------------------

} Table ;

typedef int RecId;

// IMPLEMENTED---------------------------------------------------------------------------------------

// Helpers
int
Alloc_NewPage(Table* table);

int
Find_FreeSpace(Table* table, int len);

int
Copy_ToFreeSpace(Table* table, byte* record, int len, RecId* rid);

int
Init_DirtyList(Table *table);

int
MarkPage_Dirty(Table *table, int pagenum);

// ---------------------------------------------------------------------------------------

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
