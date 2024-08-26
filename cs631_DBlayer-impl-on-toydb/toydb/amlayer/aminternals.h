/// Forward declarations

int AM_CreateIndex(
    char *fileName,/* Name of indexed file */
    int indexNo,/*number of this index for file */
    char attrType,/* 'c' for char ,'i' for int ,'f' for float */
    int attrLength /* 4 for 'i' or 'f', 1-255 for 'c' */
);


int AM_DestroyIndex(
    char *fileName,/* name of indexed file */
    int indexNo /* number of this index for file */
);

int AM_DeleteEntry(
    int fileDesc, /* file Descriptor */
    char attrType, /* 'c' , 'i' or 'f' */
    int attrLength, /* 4 for 'i' or 'f' , 1-255 for 'c' */
    char *value,/* Value of key whose corr recId is to be deleted */
    int recId /* id of the record to delete */
);

int AM_InsertEntry(
    int fileDesc, /* file Descriptor */
    char attrType, /* 'i' or 'c' or 'f' */
    int attrLength, /* 4 for 'i' or 'f', 1-255 for 'c' */
    char *value, /* value to be inserted */
    int recId /* recId to be inserted */
);

void AM_PrintError(char *s);

int
AM_InsertintoLeaf(
    char *pageBuf,/* buffer where the leaf page resides */
    int attrLength,
    char *value,/* attribute value to be inserted*/
    int recId,/* recid of the attribute to be inserted */
    int index,/* index where key is to be inserted */
    int status/* Whether key is a new key or an old key */
);
void
AM_InsertToLeafFound(
    char *pageBuf,
    int recId,
    int index,
    AM_LEAFHEADER *header);

void
AM_InsertToLeafNotFound(
    char *pageBuf,
    char *value,
    int recId,
    int index,
    AM_LEAFHEADER *header);

void
AM_Compact(
    int low,
    int high,
    char *pageBuf,
    char *tempPage,
    AM_LEAFHEADER *header
);


int
AM_SplitLeaf(int fileDesc, /* file descriptor */
             char *pageBuf, /* pointer to buffer */
             int *pageNum, /* pagenumber of new leaf created */
             int attrLength,
             int recId,
             char *value, /* attribute value for insert */

             int status, /* Whether key was found or not in the tree */
             int index, /* place where key is to be inserted */
             char *key /* returns the key to be filled in the parent */
            );
int
AM_AddtoParent(
    int fileDesc,
    int pageNum, /* page Number to be added to parent */
    char *value, /*  pointer to attribute value to be added -
				gives back the attribute value to be added to it's parent*/
    int attrLength
);

void
AM_AddtoIntPage(
    char *pageBuf,
    char *value, /* value to be added to the node */
    int pageNum, /* page number of child to be inserted */
    int offset, /* place where key is to be inserted */
    AM_INTHEADER *header
);

void
AM_FillRootPage(
    char *pageBuf,/* buffer to new root */
    int pageNum1,
    int pageNum2,/* pagenumbers of it;s two children*/
    char *value, /* attr value to be inserted */
    short attrLength,
    short maxKeys /* some info about the header */
);

void
AM_SplitIntNode(
    char *pageBuf,/* internal node to be split */
    char *pbuf1,
    char *pbuf2, /* the buffers for the two halves */
    AM_INTHEADER *header,
    char *value, /*  pointer to key to be added and to be returned to parent*/
    int pageNum,
    int offset
);

void
AM_PushStack(int pageNum, int offset);

void
AM_PopStack();

void
AM_topofStack(int *pageNum,
              int *offset
             );
void
AM_EmptyStack();

int
AM_Search(
    int fileDesc,
    char attrType,
    int attrLength,
    char *value,
    int *pageNum, /* page number of page where key is present or can be inserted*/
    char **pageBuf, /* pointer to buffer in memory where leaf page corresponding                                                        to pageNum can be found */
    int *indexPtr /* pointer to index in leaf where key is present or
			    can be inserted */
);

int
AM_BinSearch(
    char *pageBuf, /* buffer where the page is found */
    char attrType,
    int attrLength,
    char *value, /* attribute value for which search is called */
    int *indexPtr,
    AM_INTHEADER *header
);

int
AM_SearchLeaf(
    char *pageBuf, /* buffer where the leaf page resides */
    char attrType,
    int attrLength,
    char *value, /* attribute value to be compared with */
    int *indexPtr,/* pointer to the index where key is found or can be inserted */
    AM_LEAFHEADER *header
);

int
AM_Compare(
    char *bufPtr,
    char attrType,
    int attrLength,
    char *valPtr
);

void
AM_PrintIntNode(
    char *pageBuf,
    char attrType
);

void
AM_PrintLeafNode(
    char *pageBuf,
    char attrType
);
int
AM_DumpLeafPages(
    int fileDesc,
    int min,
    int attrLength,
    char attrType
);
void
AM_PrintLeafKeys(
    char *pageBuf,
    char attrType
);

void
AM_PrintAttr(
    char *bufPtr,
    char attrType,
    int attrLength
);

void
AM_PrintTree(
    int fileDesc,
    int pageNum,
    char attrType
);


