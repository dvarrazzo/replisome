#ifndef _RELDATA_H_
#define _RELDATA_H_

#include "postgres.h"

#include "utils/hsearch.h"
#include "utils/rel.h"


/* forward declarations */
struct InclusionCommand;


typedef struct JsonRelationEntry
{
	Oid relid;

	/* should this table be emitted? If both are false we don't know yet */
	bool include;
	bool exclude;

	/* who chose to include this table?
	 * Can be NULL if configuration is pretty much empty. */
	struct InclusionCommand *chosen_by;

	int *colidxs;               /* indexes of columns to emit into tupdesc */
	int *keyidxs;               /* indexes of attributes to emit into tupdesc */

	bool names_emitted;         /* true if table names have been emitted */
	bool key_emitted;           /* true if table key names have been emitted */

	/* pre-calculated fields in the output format */
	char *keynames;
	char *keytypes;
	char *colnames;
	char *coltypes;

	/* Compiled structures to filter records */
	Node *row_filter;
	struct ExprState *exprstate;
	struct EState *estate;

} JsonRelationEntry;


HTAB *reldata_create(void);
JsonRelationEntry *reldata_find(HTAB *reldata, Oid relid);
JsonRelationEntry *reldata_enter(HTAB *reldata, Oid relid);
bool reldata_remove(HTAB *reldata, Oid relid);

void reldata_to_invalidate(HTAB *reldata);
void reldata_invalidate(Datum arg, Oid relid);

void reldata_complete(JsonRelationEntry *entry, Relation relation,
	bool pretty_print);

#endif
