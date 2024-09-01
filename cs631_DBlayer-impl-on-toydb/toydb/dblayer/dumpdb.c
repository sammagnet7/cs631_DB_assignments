#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(ret_val)        \
    {                        \
        if (ret_val < 0)         \
        {                    \
            PF_PrintError(); \
            exit(1);         \
        }                    \
    }

void printRow(void *callbackObj, RecId rid, byte *row, int len)
{    
    Schema *schema = (Schema *)callbackObj;
    byte *cursor = row;
    
// IMPLEMENTED---------------------------------------------------------------------------------------

    int byteOffset = 0;
    for (int i = 0; i < schema->numColumns; i++)
    {
        ColumnDesc *colDesc = schema->columns[i];
        // switch corresponding schema type is
        switch (colDesc->type)
        {

        // VARCHAR : DecodeCString
        case VARCHAR:
            short length = DecodeShort(row + byteOffset);            // Get the string length from the 2 byte short in front of it
            char *str = (char *)malloc(sizeof(char) * (length + 1)); // +1 accounts for the null byte
            DecodeCString(row + byteOffset, str, length + 1);
            printf("%s,", str);
            free(str);
            byteOffset += length + 2; // +2 accounts for 2 byte short that stores length of the string
            break;
        // INT : DecodeInt
        case INT:
            printf("%d", DecodeInt(row + byteOffset));
            byteOffset += 4;
            break;
        // LONG: DecodeLong
        case LONG:
            printf("%lld", DecodeLong(row + byteOffset));
            byteOffset += 8;
            break;
        }
    }
    printf("\n");

// ---------------------------------------------------------------------------------------
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"

void index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value)
{   
// IMPLEMENTED---------------------------------------------------------------------------------------

    // Open index ...
    int scanDesc = AM_OpenIndexScan(indexFD, 'i', 4, op, (char *)&value);
    RecId rid;
    
    int max_len;
    while (true)
    {
        // find next entry in index
        rid = AM_FindNextEntry(scanDesc);
        
        if (rid != AME_EOF) // If next entry exists
        {
            byte *record = (byte*)calloc(INPAGE_MAXPOSS_RECORD_SIZE, sizeof(byte));
            // fetch rid from table
            max_len = Table_Get(tbl, rid, record, INPAGE_MAXPOSS_RECORD_SIZE);
            // Dump the row
            printRow(schema, rid, record, max_len);
            free(record);
        }
        else
            break;
    }
    // close index ...
    AM_CloseIndexScan(scanDesc);

// ---------------------------------------------------------------------------------------
}

int main(int argc, char **argv)
{
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    Table *tbl;
    int ret_val;

// IMPLEMENTED---------------------------------------------------------------------------------------
    ret_val = Table_Open(DB_NAME, schema, false, &tbl);
    checkerr(ret_val);
// ---------------------------------------------------------------------------------------

    if (argc == 2 && *(argv[1]) == 's')
    {   
// IMPLEMENTED---------------------------------------------------------------------------------------
        // invoke Table_Scan with printRow, which will be invoked for each row in the table.
        Table_Scan(tbl, schema, printRow);
// ---------------------------------------------------------------------------------------        
    }
    else
    {
        // index scan by default
        int indexFD = PF_OpenFile(INDEX_NAME);
        checkerr(indexFD);

        // Ask for populations less than 100000, then more than 100000. Together they should
        // yield the complete database.
        index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
        index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
    }
    Table_Close(tbl);
}
