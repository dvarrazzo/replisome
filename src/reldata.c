#include "reldata.h"

#include "jsonbutils.h"
#include "includes.h"

HTAB *
reldata_create()
{
	HTAB *reldata;
	HASHCTL		ctl;

	reldata = palloc0(sizeof(reldata));

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(JsonRelationEntry);
	ctl.hash = oid_hash;
	reldata = hash_create(
		"json relations cache", 32, &ctl, HASH_ELEM | HASH_FUNCTION);

	return reldata;
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

	if (entry->columns)
		pfree(entry->columns);

	hash_search(reldata, (void *)&(oid), HASH_REMOVE, NULL);
	return true;
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
		elog(DEBUG1, "reldata will be invalidated");
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


void
reldata_complete(JsonRelationEntry *entry, Relation relation,
	struct InclusionCommand *chosen_by)
{
	TupleDesc tupdesc;
	int natt;
	Form_pg_attribute attr;

	if (!chosen_by)
		return;

	tupdesc = RelationGetDescr(relation);

	if (chosen_by->columns) {
		int i, ncols;
		ncols = jbu_array_len(chosen_by->columns);
		for (i = 0; i < ncols; i ++) {
			char *want = jbu_getitem_str(chosen_by->columns, i);

			for (natt = 0; natt < tupdesc->natts; natt++) {
				attr = tupdesc->attrs[natt];
				if (attr->attisdropped || attr->attnum < 0)
					continue;

				if (strcmp(NameStr(attr->attname), want) == 0) {
					elog(DEBUG1, "want column %s is the number %i",
						want, natt);
					entry->columns = bms_add_member(entry->columns, natt);
					break;
				}
			}
		}
	}

	if (chosen_by->skip_columns) {
		int i, ncols;

		/* Select all the valid columns */
		for (natt = 0; natt < tupdesc->natts; natt++) {
			attr = tupdesc->attrs[natt];
			if (attr->attisdropped || attr->attnum < 0)
				continue;

			entry->columns = bms_add_member(entry->columns, natt);
		}

		ncols = jbu_array_len(chosen_by->skip_columns);
		for (i = 0; i < ncols; i ++) {
			char *want = jbu_getitem_str(chosen_by->skip_columns, i);

			for (natt = 0; natt < tupdesc->natts; natt++) {
				attr = tupdesc->attrs[natt];
				if (attr->attisdropped || attr->attnum < 0)
					continue;

				if (strcmp(NameStr(attr->attname), want) == 0) {
					elog(DEBUG1, "unwanted column %s is the number %i",
						want, natt);
					entry->columns = bms_del_member(entry->columns, natt);
					break;
				}
			}
		}
	}
}
