# include <stdio.h>
# include "am.h"
# include "pf.h"

# define AM_MAXSTACK 50

struct {
    int pageNumber;
    int offset;
} AM_Stack[AM_MAXSTACK];

int AM_topofStackPtr = -1;

void
AM_PushStack(int pageNum, int offset)
{
    AM_topofStackPtr++;
    AM_Stack[AM_topofStackPtr].pageNumber  = pageNum;
    AM_Stack[AM_topofStackPtr].offset  = offset;
}

void
AM_PopStack()
{
    AM_topofStackPtr--;
}

void
AM_topofStack(int *pageNum,
              int *offset
             )
{
    *pageNum = AM_Stack[AM_topofStackPtr].pageNumber ;
    *offset = AM_Stack[AM_topofStackPtr].offset ;
}

void
AM_EmptyStack()
{
    AM_topofStackPtr = -1;
}

