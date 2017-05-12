#include "reldata.h"

#include "jsonbutils.h"
#include "includes.h"
#include "executor.h"

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/syscache.h"
#include "access/htup_details.h"


HTAB *
reldata_create(MemoryContext ctx)
{
	HTAB *reldata;
	HASHCTL		ctl;

	reldata = palloc0(sizeof(reldata));

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(JsonRelationEntry);
	ctl.hash = oid_hash;
	ctl.hcxt = ctx;
	reldata = hash_create(
		"json relations cache", 32, &ctl,
		HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	return reldata;
}

void
reldata_destroy(HTAB *reldata)
{
	HASH_SEQ_STATUS status;
	JsonRelationEntry *entry;

	if (reldata == NULL)
		return;

	hash_seq_init(&status, reldata);
	while ((entry = hash_seq_search(&status)) != NULL) {
		reldata_free(entry);
	}

	hash_destroy(reldata);
}

JsonRelationEntry *
reldata_find(HTAB *reldata, Oid relid)
{
	JsonRelationEntry *entry;
	entry = (JsonRelationEntry *)hash_search(
		reldata, (void *)&(relid), HASH_FIND, NULL);
	return entry;
}


JsonRelationEntry *
reldata_enter(HTAB *reldata, Oid relid)
{
	JsonRelationEntry *entry;
	bool found;

	entry = (JsonRelationEntry *)hash_search(
		reldata, (void *)&(relid), HASH_ENTER, &found);

	if (!found)
	{
		elog(DEBUG1, "entry for relation %u is new", relid);
		memset(entry, '\0', sizeof(JsonRelationEntry));
		entry->relid = relid;
	}

	return entry;
}


bool
reldata_remove(HTAB *reldata, Oid oid)
{
	JsonRelationEntry *entry;

	entry = reldata_find(reldata, oid);
	if (!entry)
		return false;

	reldata_free(entry);
	hash_search(reldata, (void *)&(oid), HASH_REMOVE, NULL);
	return true;
}


void
reldata_free(JsonRelationEntry *entry)
{
	if (entry->colidxs)
		pfree(entry->colidxs);
	if (entry->keyidxs)
		pfree(entry->keyidxs);

	if (entry->keynames)
		pfree(entry->keynames);
	if (entry->keytypes)
		pfree(entry->keytypes);
	if (entry->colnames)
		pfree(entry->colnames);
	if (entry->coltypes)
		pfree(entry->coltypes);

	if (entry->estate)
		FreeExecutorState(entry->estate);
}


/* The hash that should receive invalidation information from relcache.
 * The invalidation callback is called more often than what we need: we only
 * need invalidating our schema if the callback is called between a begin_cb
 * and a commit_cb. We cannot register dynamically the invalidation callback
 * (there is no API to remove a callback) so we use a static pointer and set
 * something there only when the callback should take effect.
 */
static HTAB *to_invalidate;


/* Set the hash table to receive invalidation callbacks.
 * If the argument is NULL invalidation callbacks invocations will be ignored.
 */
void
reldata_to_invalidate(HTAB *reldata)
{
	if (reldata)
		elog(DEBUG1, "reldata will be invalidated at %p", reldata);
	else
		elog(DEBUG1, "invalidation will be ignored");

	to_invalidate = reldata;
}


/* Remove a table from the reldata to invalidate.
 * This function is a callback to register with CacheRegisterRelcacheCallback
 * to receive invalidation information.
 */
void
reldata_invalidate(Datum arg, Oid relid)
{
	if (to_invalidate == NULL)
		return;

	if (reldata_remove(to_invalidate, relid)) {
		elog(DEBUG1, "entry for relation %u removed", relid);
	}
}


static void find_columns_to_emit(JsonRelationEntry *entry,
	TupleDesc tupdesc, TupleDesc indexdesc, int **dest);
static void fill_output_fields(JsonRelationEntry *entry, TupleDesc tupdesc,
	bool replident, bool pretty_print);


/* Complete the configuration of a relation description.
 * Assume chosen_by is set to the config entry that selected this table. */
void
reldata_complete(JsonRelationEntry *entry, Relation relation,
	bool pretty_print)
{
	Relation indexrel;
	TupleDesc tupdesc;

	tupdesc = RelationGetDescr(relation);
	find_columns_to_emit(entry, tupdesc, NULL, &entry->colidxs);
	fill_output_fields(entry, tupdesc, false, pretty_print);

	indexrel = RelationIdGetRelation(relation->rd_replidindex);
	if (indexrel != NULL)
	{
		TupleDesc indexdesc = RelationGetDescr(indexrel);
		find_columns_to_emit(
			entry, tupdesc, indexdesc, &entry->keyidxs);
		fill_output_fields(entry, tupdesc, true, pretty_print);
		RelationClose(indexrel);
	}

	if (entry->chosen_by && entry->chosen_by->row_filter) {
		entry->row_filter = parse_row_filter(
			relation, entry->chosen_by->row_filter);
		entry->exprstate = prepare_row_filter(entry->row_filter);
		entry->estate = create_estate_for_relation(relation, false);
	}
}

static void
fill_output_fields(JsonRelationEntry *entry, TupleDesc tupdesc,
	bool replident, bool pretty_print)
{
	StringInfoData colnames, coltypes;
	int *attrlist;
	char *comma = "";
	int natt;
	int *pattr;

	initStringInfo(&colnames);
	initStringInfo(&coltypes);

	if (replident && entry->keyidxs)
		attrlist = entry->keyidxs;
	else
		attrlist = entry->colidxs;

	/* Print column information (name, type, value) */
	for (pattr = attrlist; (natt = *pattr) >= 0; pattr++)
	{
		Form_pg_attribute	attr;		/* the attribute itself */
		Oid					typid;		/* type of current attribute */
		HeapTuple			type_tuple;	/* information about a type */
		Form_pg_type		type_form;

		attr = tupdesc->attrs[natt];
		typid = attr->atttypid;

		/* Figure out type name */
		type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);
		type_form = (Form_pg_type) GETSTRUCT(type_tuple);

		/* Accumulate each column info */
		appendStringInfo(&colnames, "%s\"%s\"", comma, NameStr(attr->attname));
		appendStringInfo(&coltypes, "%s\"%s\"", comma, NameStr(type_form->typname));

		ReleaseSysCache(type_tuple);

		/* The first column does not have comma */
		if (comma[0] == '\0')
			comma = pretty_print ? ", " : ",";
	}

	if (replident) {
		entry->keynames = colnames.data;
		entry->keytypes = coltypes.data;
	}
	else {
		entry->colnames = colnames.data;
		entry->coltypes = coltypes.data;
	}
}

static void
find_columns_to_emit(JsonRelationEntry *entry,
	TupleDesc tupdesc, TupleDesc indexdesc, int **dest)
{
	int natt;
	int *pdest;

	*dest = palloc(sizeof(int) * (tupdesc->natts + 1));

	/* Print column information (name, type, value) */
	for (natt = 0, pdest = *dest; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr = tupdesc->attrs[natt];

		/* Do not print dropped or system columns */
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		/* Search indexed columns in whole heap tuple */
		if (indexdesc != NULL)
		{
			bool found_col = false;
			int j;
			for (j = 0; j < indexdesc->natts; j++)
			{
				if (0 == strcmp(
					NameStr(attr->attname),
					NameStr(indexdesc->attrs[j]->attname)))
				{
					found_col = true;
					break;
				}
			}

			/* Print only indexed columns */
			if (!found_col) {
				continue;
			}
		}

		/* Do not print columns skipped by the user */
		if (!inc_include_column(entry->chosen_by, NameStr(attr->attname))) {
			elog(DEBUG1,
				"attribute \"%s\" ignored by column selection",
				NameStr(attr->attname));
			continue;
		}

		/* we got this column */
		*pdest++ = natt;
	}

	*pdest = -1;     /* sentinel */
}
