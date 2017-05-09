#include "reldata.h"

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
		entry->include = false;
		entry->exclude = false;
		entry->names_emitted = false;
		entry->key_emitted = false;
	}

	return entry;
}


JsonRelationEntry *
reldata_remove(HTAB *reldata, Oid oid)
{
	JsonRelationEntry *entry;
	entry = (JsonRelationEntry *)hash_search(
		reldata, (void *)&(oid), HASH_REMOVE, NULL);
	return entry;
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
	JsonRelationEntry *entry;

	if (to_invalidate == NULL)
		return;

	entry = reldata_remove(to_invalidate, relid);
	if (entry) {
		elog(DEBUG1, "entry for relation %u removed", relid);
	}
}
