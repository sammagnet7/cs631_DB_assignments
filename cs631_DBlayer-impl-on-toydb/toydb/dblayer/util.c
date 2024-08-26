#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "tbl.h"
#include "util.h"

char *trim(char *str)
{
  char *end;

  // Trim leading space
  while(isspace((unsigned char)*str)) str++;

  if(*str == 0)  // All spaces?
    return str;

  // Trim trailing space
  end = str + strlen(str) - 1;
  while(end > str && isspace((unsigned char)*end)) end--;

  // Write new null terminator character
  end[1] = '\0';

  return str;
}


/**
  split takes a character buf, and uses strtok to populate
  an array of token*'s. 
*/
int 
split(char *buf, char *delim, char **tokens) {
    char *token = strtok(buf, delim);
    int n = 0;
    while(token) {
	tokens[n] = trim(token);
	token = strtok(NULL, delim);
	n++;
    }
    return n;
}

Schema *
parseSchema(char *buf) {
    buf = strdup(buf);
    char *tokens[MAX_TOKENS];
    int n = split(buf, ",", tokens);
    Schema *sch = malloc(sizeof(Schema));
    sch->columns = malloc(n * sizeof(ColumnDesc *));
    // strtok is terrible; it depends on global state.
    // Do one split based on ',".
    // Could use strtok_s for this use case
    char *descTokens[MAX_TOKENS];
    sch->numColumns = n;
    for (int i = 0; i < n; i++) {
	int c = split(tokens[i],":", descTokens);
	assert(c == 2);
	ColumnDesc *cd = (ColumnDesc *) malloc(sizeof(ColumnDesc));
	cd->name = strdup(descTokens[0]);
	char *type = descTokens[1];
	int itype = 0;
	if (stricmp(type, "varchar") == 0) {
	    itype = VARCHAR;
	} else if (stricmp(type, "int") == 0) {
	    itype = INT;
	} else if (stricmp(type, "long") == 0) {
	    itype = LONG;
	} else {
	    fprintf(stderr, "Unknown type %s \n", type);
	    exit(EXIT_FAILURE);
	}
	cd->type = itype;
	sch->columns[i] = cd;
    }
    free(buf);
    return sch;
}

