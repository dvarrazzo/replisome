#ifndef _INCLUDES_H_
#define _INCLUDES_H_

#include "postgres.h"

#include "lib/ilist.h"
#include "nodes/parsenodes.h"
#include "regex/regex.h"
#include "utils/relcache.h"

typedef enum
{
	CMD_INCLUDE_ALL,
	CMD_INCLUDE_TABLE,
	CMD_INCLUDE_TABLE_PATTERN,
	CMD_EXCLUDE_TABLE,
	CMD_EXCLUDE_TABLE_PATTERN,
} CommandType;


typedef struct
{
	char		*schema_name;		/* name of schema to include/exclude */
	char		*table_name;		/* name of table to include/exclude */
	regex_t		table_re;			/* pattern of table names include/exclude */
	dlist_node	node;				/* double-linked list */
	CommandType	type;				/* what command is this? */
	Datum		columns;			/* columns to include as jsonb list */
	Datum		skip_columns;		/* columns to ignore as jsonb list */
} InclusionCommand;


typedef struct
{
	dlist_head	head;				/* commands included in the list */
} InclusionCommands;


void inc_parse_include(DefElem *elem, InclusionCommands **cmds);
void inc_parse_exclude(DefElem *elem, InclusionCommands **cmds);
bool inc_should_emit(InclusionCommands *cmds, Relation relation,
		InclusionCommand **chosen_by);


#endif
