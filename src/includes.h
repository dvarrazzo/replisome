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
	CMD_INCLUDE_TABLES,
	CMD_EXCLUDE_TABLES,
} CommandType;


typedef struct InclusionCommand
{
	int			num;				/* number of the command (for debug) */
	CommandType	type;				/* what command is this? */
	dlist_node	node;				/* double-linked list */

	char		*table_name;		/* name of table to include/exclude */
	regex_t		*table_re;			/* pattern of table names include/exclude */
	char		*schema_name;		/* name of schema to include/exclude */
	regex_t		*schema_re;			/* pattern of schema names include/exclude */
	Datum		columns;			/* columns to include as jsonb list */
	Datum		skip_columns;		/* columns to ignore as jsonb list */
	char		*row_filter;		/* only emit records matching this check */
} InclusionCommand;


typedef struct
{
	dlist_head	head;				/* commands included in the list */
} InclusionCommands;


void inc_parse_include(DefElem *elem, InclusionCommands **cmds);
void inc_parse_exclude(DefElem *elem, InclusionCommands **cmds);
bool inc_should_emit(InclusionCommands *cmds, Relation relation,
		InclusionCommand **chosen_by);
bool inc_include_column(InclusionCommand *cmd, const char *name);


#endif
