
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
    // TODO: Handle overwrite
    // Initialize PF, create PF file,
    PF_Init();
    int rc, file_descriptor, pagenum;
    char* pagebuf;
    Table* tableHandle;

    file_descriptor = PF_OpenFile(dbname);
    if (file_descriptor < 0) {
        
        rc = PF_CreateFile(dbname);// Attempt to create new file
        if (rc != PFE_OK) {
            PF_PrintError("PF_CreateFile");
            return rc;  // Return the error code
        }

        file_descriptor = PF_OpenFile(dbname); // Now try opening the newly created file
        if (file_descriptor < 0) {
            PF_PrintError("PF_OpenFile");
            return file_descriptor;  // Return the error code
        }
    }// After this file_descriptor is having a valid value
    
    // allocate Table structure  and initialize and return via ptable
    tableHandle = (Table*)malloc(sizeof(Table)); // Should we check for failed allocation??
    tableHandle->schema = schema;
    tableHandle->file_descriptor = file_descriptor;
    tableHandle->dirtyPageList = Init_DirtyList(tableHandle);
    tableHandle->dirtyListSize = 0;
    rc = PF_GetFirstPage(file_descriptor, &pagenum, &pagebuf);

    if (rc == PFE_OK) {
        tableHandle->firstPageNum = pagenum;  // Store the first page number
    } else if (rc != PFE_EOF) {
        // If there's an error other than EOF, close the file and return error
        PF_PrintError("PF_GetFirstPage");
        PF_CloseFile(file_descriptor);
        free(tableHandle);
        return rc;  // Return the error code
    }
    
    *ptable = tableHandle; // Return the initialized Table structure
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage.
    return 0;
}

void Table_Close(Table *tbl)
{
    if (tbl == NULL) {
        return; // Nothing to close
    }
    // Unfix any dirty pages, close file.
    DirtyPageNode *current = tbl->dirtyPageList;
    while (current != NULL) {
        int pageNum = current->pageNum;
        
        // Unfix the page with the "dirty" flag set to true
        int rc = PF_UnfixPage(tbl->file_descriptor, pageNum, TRUE);
        if (rc != PFE_OK) {
            PF_PrintError("PF_UnfixPage");
        }

        // Move to the next node
        DirtyPageNode *temp = current;
        current = current->next;
        free(temp);  // Free the current node
    }
    
    int rc = PF_CloseFile(tbl->file_descriptor);
    if (rc != PFE_OK) {
        PF_PrintError("PF_CloseFile");
    }

    // Free the Table struct itself
    free(tbl);
}

int Table_Insert(Table *tbl, byte *record, int len, RecId *rid)
{
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
}

#define checkerr(err)           \
    {                           \
        if (err < 0)            \
        {                       \
            PF_PrintError();    \
            exit(EXIT_FAILURE); \
        }                       \
    }

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int Table_Get(Table *tbl, RecId rid, byte *record, int maxlen)
{
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    UNIMPLEMENTED;
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    return len; // return size of record
}

void Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn)
{

    UNIMPLEMENTED;

    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
}

// Helpers
int Init_DirtyList(Table *table) {
    if (table == NULL) {
        return -1; // Error: Invalid table
    }

    table->dirtyPageList = NULL;

    return 0; // Success
}

int MarkPage_Dirty(Table *table, int pagenum) {
    if (table == NULL) {
        return -1; // Error: Invalid table
    }

    // Check if the page is already in the list
    DirtyPageNode *current = table->dirtyPageList;
    while (current != NULL) {
        if (current->pageNum == pagenum) {
            return 0; // Page is already marked as dirty
        }
        current = current->next;
    }

    // Create a new node for the dirty page
    DirtyPageNode *newNode = (DirtyPageNode *)malloc(sizeof(DirtyPageNode));
    if (newNode == NULL) {
        return -1; // Error: Memory allocation failure
    }

    newNode->pageNum = pagenum;
    newNode->next = table->dirtyPageList;
    table->dirtyPageList = newNode;
    table->dirtyListSize++;

    return 0; // Success
}