/* pf.h: externs and error codes for Paged File Interface*/
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

/************** Error Codes *********************************/
#define PFE_OK		0	/* OK */
#define PFE_NOMEM	-1	/* no memory */
#define PFE_NOBUF	-2	/* no buffer space */
#define PFE_PAGEFIXED 	-3	/* page already fixed in buffer */
#define PFE_PAGENOTINBUF -4	/* page to be unfixed is not in the buffer */
#define PFE_UNIX	-5	/* unix error */
#define PFE_INCOMPLETEREAD -6	/* incomplete read of page from file */
#define PFE_INCOMPLETEWRITE -7	/* incomplete write of page to file */
#define PFE_HDRREAD	-8	/* incomplete read of header from file */
#define PFE_HDRWRITE	-9	/* incomplte write of header to file */
#define PFE_INVALIDPAGE -10	/* invalid page number */
#define PFE_FILEOPEN	-11	/* file already open */
#define	PFE_FTABFULL	-12	/* file table is full */
#define PFE_FD		-13	/* invalid file descriptor */
#define PFE_EOF		-14	/* end of file */
#define PFE_PAGEFREE	-15	/* page already free */
#define PFE_PAGEUNFIXED	-16	/* page already unfixed */

/* Internal error: please report to the TA */
#define PFE_PAGEINBUF	-17	/* new page to be allocated already in buffer */
#define PFE_HASHNOTFOUND -18	/* hash table entry not found */
#define PFE_HASHPAGEEXIST -19	/* page already exist in hash table */


/* page size */
#define PF_PAGE_SIZE	4096

/* externs from the PF layer */
extern int PFerrno;		/* error number of last error */
extern void PF_Init();
extern void PF_PrintError();

/****************************************************************************
PF_CreateFile:
	Create a paged file called "fname". The file should not have
	already existed before.
RETURN VALUE:
	PFE_OK	if OK
	PF error code if error.
*****************************************************************************/
int PF_CreateFile(char *fname /* name of file to create */);

/****************************************************************************
PF_DestroyFile:
	Destroy the paged file whose name is "fname". The file should
	exist, and should not be already open.

RETURN VALUE:
	PFE_OK 	if success
	PF error codes if error
*****************************************************************************/
int PF_DestroyFile(char *fname /* file name to destroy */);


/****************************************************************************
PF_OpenFile:
	Open the paged file whose name is fname.  It is possible to open
	a file more than once. Warning: Openinging a file more than once for
	write operations is not prevented. The possible consequence is
	the corruption of the file structure, which will crash
	the Paged File functions. On the other hand, opening a file
	more than once for reading is OK.

RETURN VALUE:
	The file descriptor, which is >= 0, if no error.
	PF error codes otherwise.

IMPLEMENTATION NOTES:
	A file opened more than once will have different file descriptors
	returned. Separate buffers are used.
*****************************************************************************/
int PF_OpenFile(char *fname		/* name of the file to open */);

int PF_CloseFile(int fd /* file descriptor to close */);

/****************************************************************************
PF_GetFirstPage
	Read the first page into memory and set *pagebuf to point to it.
	Set *pagenum to the page number of the page read.
	The page read is fixed in the buffer until it is unixed with
	PFunfix().

RETURN VALUE:
	PFE_OK	if no error.
	PFE_EOF	if end of file reached.(meaning there is no first page. )
	other PF error code if other error.
*****************************************************************************/
int PF_GetFirstPage(
    int fd,	/* file descriptor */
    int *pagenum,	/* page number of first page */
    char **pagebuf	/* pointer to the pointer to buffer */
);

/****************************************************************************
PF_GetNextPage:
	Read the next valid page after *pagenum, the current page number,
	and set *pagebuf to point to the page data. Set *pagenum
	to be the new page number. The new page is fixed in memory
	until PFunfix() is called.
	Note that PF_GetNextPage() with *pagenum == -1 will return the
	first valid page. PFgetFirst() is just a short hand for this.

RETURN VALUE:
	PFE_OK	if success
	PFE_EOF	if end of file reached without encountering
		any used page data.
	PFE_INVALIDPAGE  if page number is invalid.
	other PF errors code for other error.

*****************************************************************************/
int PF_GetNextPage(
    int fd,	/* file descriptor of the file */
    int *pagenum,	/* old page number on input, new page number on output */
    char **pagebuf	/* pointer to pointer to buffer of page data */
);


/****************************************************************************
PF_GetThisPage
	Read the page specifeid by "pagenum" and set *pagebuf to point
	to the page data. The page number should be valid.
RETURN VALUE:
	PFE_OK	if no error.
	PFE_INVALIDPAGE if invalid page number is specified.
	PFE_PAGEFIXED if page already fixed in memory. In this case,
		*pagebuf  is still set to point to the buffer that contains
		the page data.
	other PF error codes if other error encountered.
*****************************************************************************/

int PF_GetThisPage(
    int fd,		/* file descriptor */
    int pagenum,	/* page number to read */
    char **pagebuf	/* pointer to pointer to page data */
);


/****************************************************************************
PF_AllocPage:
	Allocate a new, empty page for file "fd".
	set *pagenum to the new page number.
	Set *pagebuf to point to the buffer for that page.
	The page allocated is fixed in the buffer.

RETURN VALUE:
	PFE_OK	if ok
	PF error codes if not ok.

*****************************************************************************/
int PF_AllocPage(
    int fd,		/* file descriptor */
    int *pagenum,	/* page number */
    char **pagebuf	/* pointer to pointer to page buffer*/
);


/****************************************************************************
PF_DisposePage:
	Dispose the page numbered "pagenum" of the file "fd".
	Only a page that is not fixed in the buffer can be disposed.

RETURN VALUE:
	PFE_OK	if no error.
	PF error code if error.
*****************************************************************************/
int PF_DisposePage(
    int fd,		/* file descriptor */
    int pagenum	/* page number */
);


/****************************************************************************
PF_UnfixPage:
	Tell the Paged File Interface that the page numbered "pagenum"
	of the file "fd" is no longer needed in the buffer.
	Set the variable "dirty" to TRUE if page has been modified.

RETURN VALUE:
	PFE_OK	if no error
	PF error code if error.

*****************************************************************************/
int PF_UnfixPage(int fd,	/* file descriptor */
                 int pagenum,	/* page number */
                 int dirty	/* true if file is dirty */
                );
