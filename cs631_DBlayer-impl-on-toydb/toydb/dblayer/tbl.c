
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err)           \
    {                           \
        if (err < 0)            \
        {                       \
            PF_PrintError();    \
            exit(EXIT_FAILURE); \
        }                       \
    }

int getLen(int slot, byte *pageBuf);
int getNumSlots(byte *pageBuf);
void setNumSlots(byte *pageBuf, int nslots);
int getNthSlotOffset(int slot, char *pageBuf);

/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    // Initialize PF, create PF file,
    PF_Init();
    if (overwrite)
    {
        PF_DestroyFile(dbname);
    }
    int rc, file_descriptor, pagenum;
    char *pagebuf;
    Table *tableHandle;

    file_descriptor = PF_OpenFile(dbname);
    if (file_descriptor < 0)
    {

        rc = PF_CreateFile(dbname); // Attempt to create new file
        if (rc != PFE_OK)
        {
            PF_PrintError("PF_CreateFile");
            return rc; // Return the error code
        }

        file_descriptor = PF_OpenFile(dbname); // Now try opening the newly created file
        if (file_descriptor < 0)
        {
            PF_PrintError("PF_OpenFile");
            return file_descriptor; // Return the error code
        }
    } // After this file_descriptor is having a valid value

    // allocate Table structure  and initialize and return via ptable
    tableHandle = (Table *)malloc(sizeof(Table)); // Should we check for failed allocation??
    tableHandle->schema = schema;
    tableHandle->file_descriptor = file_descriptor;
    tableHandle->dirtyPageList = Init_DirtyList(tableHandle);
    tableHandle->dirtyListSize = 0;
    rc = PF_GetFirstPage(file_descriptor, &pagenum, &pagebuf);

    if (rc == PFE_OK)
    {
        tableHandle->firstPageNum = pagenum; // Store the first page number
        tableHandle->currentPageNum = pagenum;
        tableHandle->pagebuf = pagebuf;
        PF_UnfixPage(tableHandle->file_descriptor, pagenum, false);
    }
    else if (rc == PFE_EOF) // Table has no pages
    {
        tableHandle->firstPageNum = pagenum; // Store the first page number as -1
        tableHandle->currentPageNum = pagenum;
        tableHandle->pagebuf = NULL;
    }
    else
    {
        // If there's an error other than EOF, close the file and return error
        PF_PrintError("PF_GetFirstPage");
        PF_CloseFile(file_descriptor);
        free(tableHandle);
        return rc; // Return the error code
    }

    *ptable = tableHandle; // Return the initialized Table structure
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage.
    return 0;
}

void Table_Close(Table *tbl)
{
    if (tbl == NULL)
    {
        return; // Nothing to close
    }
    // Unfix any dirty pages, close file.
    DirtyPageNode *current = tbl->dirtyPageList;
    while (current != NULL)
    {
        int pageNum = current->pageNum;

        // Unfix the page with the "dirty" flag set to true
        int rc = PF_UnfixPage(tbl->file_descriptor, pageNum, TRUE);
        if (rc != PFE_OK)
        {
            PF_PrintError("PF_UnfixPage");
        }

        // Move to the next node
        DirtyPageNode *temp = current;
        current = current->next;
        free(temp); // Free the current node
    }

    int rc = PF_CloseFile(tbl->file_descriptor);
    if (rc != PFE_OK)
    {
        PF_PrintError("PF_CloseFile");
    }

    // Free the Table struct itself
    free(tbl);
}

int Table_Insert(Table *tbl, byte *record, int len, RecId *rid)
{
    int rc;
    // Check if Table has no pages
    if (tbl->currentPageNum == -1 && tbl->firstPageNum == -1 && tbl->pagebuf == NULL)
    {
        rc = Alloc_NewPage(tbl);
        if (rc < 0)
            return rc;
        tbl->firstPageNum = tbl->currentPageNum;
    }
    else
    {
        rc = Find_FreeSpace(tbl, len);
        if (rc == -1 || rc == PFE_EOF) // Couldn't find required freespace in existing pages
        {
            rc = Alloc_NewPage(tbl); // Allocate a fresh page if len is not enough for remaining space
            if (rc < 0)
                return rc;
        }
    }
    // Get the next free slot on page, and copy record in the free space
    // Update slot and free space index information on top of page.
    // Also unfixes the page
    Copy_ToFreeSpace(tbl, record, len, rid);
}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int Table_Get(Table *tbl, RecId rid, byte *record, int maxlen)
{
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;
    int len;
    char *pagebuf;
    PageHeader *header;
    int rc = PF_GetThisPage(tbl->file_descriptor, pageNum, &pagebuf);
    if (rc != PFE_OK && rc != PFE_PAGEFIXED)
    {
        PF_PrintError("TABLE_GET ");
        return 0;
    }
    // In the page get the slot offset of the record, and
    header = PAGE_HEADER(pagebuf);
    int offset = GET_OFFSET_AT_SLOT(header, slot);
    int recordSize = RECORD_SIZE_AT_SLOT(header, slot);
    // memcpy bytes into the record supplied.
    if (recordSize > maxlen)
        memcpy(record, &pagebuf[offset], maxlen);
    else
        memcpy(record, &pagebuf[offset], recordSize);
    // Unfix the page
    PF_UnfixPage(tbl->file_descriptor, pageNum, false);
    return recordSize; // return size of record
}

void Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn)
{
    int pagenum = -1, rc, recordLen;
    char *pagebuf;
    RecId recID;
    byte record[MAX_RECORD_SIZE];
    PageHeader *header;
    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    while (1)
    {
        rc = PF_GetNextPage(tbl->file_descriptor, &pagenum, &pagebuf);
        if (rc == PFE_EOF)
            break;
        if (rc == PFE_OK)
        {
            header = PAGE_HEADER(pagebuf);
            PF_UnfixPage(tbl->file_descriptor, pagenum, false);
            //    for each record in that page,
            for (int i = 0; i < header->numRecords; i++)
            {
                recID = BUILD_RECORD_ID(pagenum, i);
                recordLen = Table_Get(tbl, recID, record, MAX_RECORD_SIZE);
                callbackfn(callbackObj, recID, record, recordLen);
            }
        }
        else
        {
            PF_PrintError("TABLE_SCAN");
            break;
        }
    }
}

// Helpers
/*
 Examines page header and attempts to find a page with freespace length atleast len
 Returns -1 if no such page is there, else returns the page number
 */
int Find_FreeSpace(Table *table, int len)
{
    if (table == NULL || len <= 0)
    {
        return -1; // Invalid input
    }

    // Initialize the page buffer and page number
    char *pagebuf = NULL;
    int pagenum = table->firstPageNum;
    PageHeader *header;
    // Get the first page
    int rc = PF_GetThisPage(table->file_descriptor, pagenum, &pagebuf);

    // Iterate through all pages in the table
    while (1)
    {
        if (rc != PFE_OK && rc != PFE_PAGEFIXED)
        {
            PF_PrintError("PF_GetThisPage");
            return rc;
        }

        // Access the page header from the page buffer
        header = PAGE_HEADER(pagebuf);

        // Calculate the amount of free space
        int freeSpaceSize = FREESPACE_SIZE(header) - ENTRY_SIZE /*To accomodate the new entry in record offset array*/;

        // Check if the free space is sufficient
        if (freeSpaceSize >= len)
        {
            table->pagebuf = pagebuf;
            return pagenum; // Found a suitable page, page is now fixed
        }
        // Unfix this page
        PF_UnfixPage(table->file_descriptor, pagenum, true);
        // Move to the next page
        rc = PF_GetNextPage(table->file_descriptor, &pagenum, &pagebuf);

        if (rc == PFE_EOF)
            break; // No more pages
    }

    // No suitable page found
    return -1;
}
/*
 Allocates a new page with and sets up page header
 Returns errorcode on error, else returns 0
 Also sets the currentpagenum and pagebuf in table
 */
int Alloc_NewPage(Table *table)
{
    int pagenum;
    char *pagebuf;
    // Allocate a new page
    int rc = PF_AllocPage(table->file_descriptor, &pagenum, &pagebuf);
    if (rc != PFE_OK)
    {
        PF_PrintError("PF_AllocPage ");
        return rc;
    }
    // Set up page header
    PageHeader *header = PAGE_HEADER(pagebuf);

    // Set the initial number of slots to 0
    header->numRecords = 0;

    // Set offset to the end of free space region by pointing at last byte
    header->freespaceoffset = PF_PAGE_SIZE - 1;

    // Update table structure
    table->currentPageNum = pagenum;
    table->pagebuf = pagebuf;
    return 0;
}

/*
 Copies record of length len to freespace region of page denoted by currentpagenum in table
 Returns errorcode on error, else returns 0
 Also updates the page header and sets the record's address to rid
*/

int Copy_ToFreeSpace(Table *table, byte *record, int len, RecId *rid)
{
    // Copy record of length len to freespace region
    PageHeader *header = PAGE_HEADER(table->pagebuf);
    memcpy(FREESPACE_REGION(header, table->pagebuf, len), record, len);
    // Update header
    int slot = NEXT_SLOT(header);
    header->recordoffset[slot] = header->freespaceoffset - len + 1;
    header->numRecords += 1;
    header->freespaceoffset -= len;
    // Mark Page as dirty
    // MarkPage_Dirty(table, table->currentPageNum);
    // Unfix the page
    PF_UnfixPage(table->file_descriptor, table->currentPageNum, TRUE);
    // Set record id
    *rid = BUILD_RECORD_ID(table->currentPageNum, slot);
    return 0;
}
/*
 Initialise the dirty list used for tracking dirty pages
*/

int Init_DirtyList(Table *table)
{
    if (table == NULL)
    {
        return -1; // Error: Invalid table
    }

    table->dirtyPageList = NULL;

    return 0; // Success
}

/*
Marks page given by pagenum as dirty in the table's dirtylist
*/
int MarkPage_Dirty(Table *table, int pagenum)
{
    if (table == NULL)
    {
        return -1; // Error: Invalid table
    }

    // Check if the page is already in the list
    DirtyPageNode *current = table->dirtyPageList;
    while (current != NULL)
    {
        if (current->pageNum == pagenum)
        {
            return 0; // Page is already marked as dirty
        }
        current = current->next;
    }

    // Create a new node for the dirty page
    DirtyPageNode *newNode = (DirtyPageNode *)malloc(sizeof(DirtyPageNode));
    if (newNode == NULL)
    {
        return -1; // Error: Memory allocation failure
    }

    newNode->pageNum = pagenum;
    newNode->next = table->dirtyPageList;
    table->dirtyPageList = newNode;
    table->dirtyListSize++;

    return 0; // Success
}