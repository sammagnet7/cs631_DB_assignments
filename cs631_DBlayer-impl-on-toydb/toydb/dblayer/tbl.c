
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
            PF_PrintError("");    \
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
    int ret_val, file_descriptor, pagenum;
    char *pagebuf;
    Table *tableHandle;

    file_descriptor = PF_OpenFile(dbname);
    if (file_descriptor < 0)
    {

        ret_val = PF_CreateFile(dbname); // Attempt to create new file
        checkerr(ret_val);

        file_descriptor = PF_OpenFile(dbname); // Now try opening the newly created file
        checkerr(file_descriptor);
    } // After this file_descriptor is having a valid value

    // Allocate Table structure  and initialize and return via ptable
    tableHandle = (Table *)malloc(sizeof(Table));
    tableHandle->schema = schema;
    tableHandle->file_descriptor = file_descriptor;

    // Attempt to get the first page of table
    ret_val = PF_GetFirstPage(file_descriptor, &pagenum, &pagebuf);

    if (ret_val == PFE_OK)
    {
        tableHandle->firstPageNum = pagenum; // Store the first page number
        tableHandle->currentPageNum = pagenum;
        tableHandle->pagebuf = NULL;
        PF_UnfixPage(tableHandle->file_descriptor, pagenum, false);

    }
    else if (ret_val == PFE_EOF) // Table has no pages
    {
        tableHandle->firstPageNum = pagenum; // Store the first page number as -1
        tableHandle->currentPageNum = pagenum;
        tableHandle->pagebuf = NULL;
    }
    else
    {
        // If there's an error other than EOF, close the file and return error
        PF_PrintError("TABLE-INSERT: PF_GetFirstPage");
        PF_CloseFile(file_descriptor);
        free(tableHandle);
        return ret_val; // Return the error code
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
    // Close file
    int ret_val = PF_CloseFile(tbl->file_descriptor);
    checkerr(ret_val);

    // Free the Table struct itself
    free(tbl);
}

int Table_Insert(Table *tbl, byte *record, int len, RecId *rid)
{
    int ret_val;
    // Check if Table has no pages
    if (tbl->currentPageNum == -1 && tbl->firstPageNum == -1 && tbl->pagebuf == NULL)
    {
        ret_val = Alloc_NewPage(tbl);
        tbl->firstPageNum = tbl->currentPageNum;
    }
    else
    {
        ret_val = Find_FreeSpace(tbl, len);
        if (ret_val == -1 || ret_val == PFE_EOF) // Couldn't find required freespace in existing pages
        {
            ret_val = Alloc_NewPage(tbl); // Allocate a fresh page if len is not enough for remaining space
        }
    }
    // Get the next free slot on page, and copy record in the free space
    // Update slot and free space index information on top of page.
    // Also unfixes the page
    Copy_ToFreeSpace(tbl, record, len, rid);
    return 0;
}

/*
  Given an record id, fill in the record (but at most maxlen bytes). Page is unfixed on exit
  Returns the number of bytes copied.
 */
int Table_Get(Table *tbl, RecId rid, byte *record, int maxlen)
{
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;
    int len;
    char *pagebuf;
    PageHeader *header;
    int ret_val = PF_GetThisPage(tbl->file_descriptor, pageNum, &pagebuf);
    if (ret_val != PFE_OK && ret_val != PFE_PAGEFIXED)
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
/*
Scans the table sequentially and calls the callbackfn on each record item
Passes callbackObj as first parameter to callbackfn
*/
void Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn)
{
    int pagenum = -1, ret_val, recordLen;
    char *pagebuf;
    RecId recID;
    byte record[INPAGE_MAX_RECORD_SPACE];
    PageHeader *header;
    // For each page obtained using PF_GetNextPage
    while (1)
    {
        ret_val = PF_GetNextPage(tbl->file_descriptor, &pagenum, &pagebuf);
        if (ret_val == PFE_EOF) // Stop if there are no more pages in table
            break;
        if (ret_val == PFE_OK)
        {
            header = PAGE_HEADER(pagebuf);
            // Unfix the page since Table_Get will be calling pf_getthispage
            PF_UnfixPage(tbl->file_descriptor, pagenum, false);
            //    for each record in that page,
            for (int i = 0; i < header->numRecords; i++)
            {
                recID = BUILD_RECORD_ID(pagenum, i);
                recordLen = Table_Get(tbl, recID, record, INPAGE_MAX_RECORD_SPACE);
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
    int ret_val = PF_GetThisPage(table->file_descriptor, pagenum, &pagebuf);

    // Iterate through all pages in the table
    while (1)
    {
        checkerr(ret_val);

        // Access the page header from the page buffer
        header = PAGE_HEADER(pagebuf);

        // Calculate the amount of free space
        int freeSpaceSize = FREESPACE_SIZE(header) - HEADER_FIELD_SIZE /*To accomodate the new entry in record offset array*/;

        // Check if the free space is sufficient
        if (freeSpaceSize >= len)
        {
            table->pagebuf = pagebuf;
            table->currentPageNum = pagenum; // Update current page number to the pagenum found, otherwise this may lead to PFE_PAGEUNFIXED
            return pagenum; // Found a suitable page, page is now fixed
        }
        // Unfix this page
        PF_UnfixPage(table->file_descriptor, pagenum, FALSE);
        // Move to the next page
        ret_val = PF_GetNextPage(table->file_descriptor, &pagenum, &pagebuf);

        if (ret_val == PFE_EOF)
            break; // No more pages
    }

    // No suitable page found
    return -1;
}
/*
 Allocates a new page with and sets up page header, page is fixed on exit
 Exits program on error, else returns 0
 Also sets the currentpagenum and pagebuf in table
 */
int Alloc_NewPage(Table *table)
{
    int pagenum;
    char *pagebuf;
    // Allocate a new page
    int ret_val = PF_AllocPage(table->file_descriptor, &pagenum, &pagebuf);
    checkerr(ret_val);
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
 Page is assumed to be fixed
 Exits program on error, else returns 0
 Also updates the page header and sets the record's address to rid
*/

int Copy_ToFreeSpace(Table *table, byte *record, int len, RecId *rid)
{
    // Copy record of length len to freespace region
    PageHeader *header = PAGE_HEADER(table->pagebuf);
    // Get freespace of len bytes in this page's buffer using its header
    memcpy(FREESPACE_INSERTION_INDEX(header, table->pagebuf, len), record, len);
    // Update header
    int slot = NEXT_SLOT(header); // Get the next empty slot
    header->recordoffset[slot] = header->freespaceoffset - len + 1; // Adds the offset to this record in slot
    header->numRecords += 1; // Added a new record
    header->freespaceoffset -= len; // Freespace shrinks by size of record in bytes
    // Unfix the page
    int ret_val  = PF_UnfixPage(table->file_descriptor, table->currentPageNum, TRUE);
    checkerr(ret_val);
    // Set record id
    *rid = BUILD_RECORD_ID(table->currentPageNum, slot);
    return 0;
}