void PFbufUnlink(
    PFbpage *bpage		/* buffer page to be unlinked from the used list */
);

int
PFbufGet(
    int fd,	/* file descriptor */
    int pagenum,	/* page number */
    PFfpage **fpage,	/* pointer to pointer to file page */
    int (*readfcn)(),	/* function to read a page */
    int (*writefcn)()	/* function to write a page */
);

int
PFbufUnfix(
    int fd,		/* file descriptor */
    int pagenum,	/* page number */
    int dirty	/* TRUE if page is dirty */
);
int
PFbufAlloc(
    int fd,		/* file descriptor */
    int pagenum,	/* page number */
    PFfpage **fpage,	/* pointer to file page */
    int (*writefcn)()
);

int
PFbufReleaseFile(
    int fd,		/* file descriptor */
    int (*writefcn)()	/* function to write a page of file */
);

int
PFbufUsed(
    int fd,		/* file descriptor */
    int pagenum	/* page number */
);

void PFbufPrint();
