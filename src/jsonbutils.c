#include "jsonbutils.h"

#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"

Datum
jbu_create(const char *s)
{
	return DirectFunctionCall1(jsonb_in, CStringGetDatum(s));
}


bool
jbu_is_type(Datum jsonb, const char *type)
{
	char *cjtype;
	Datum jtype;
	bool rv;

	jtype = DirectFunctionCall1(jsonb_typeof, jsonb);
	cjtype = TextDatumGetCString(jtype);
	rv = (strcmp(cjtype, type) == 0);
	pfree(cjtype);
	pfree(DatumGetPointer(jtype));
	return rv;
}

int
jbu_array_len(Datum jsonb)
{
	return DatumGetInt32(DirectFunctionCall1(jsonb_array_length, jsonb));
}


char *
jbu_getattr_str(Datum jsonb, const char *attr)
{
	char *rv = NULL;
	Datum dattr = CStringGetTextDatum(attr);

	if (DatumGetBool(DirectFunctionCall2(jsonb_exists, jsonb, dattr))) {
		Datum drv = DirectFunctionCall2(jsonb_object_field_text, jsonb, dattr);
		rv = TextDatumGetCString(drv);
		pfree(DatumGetPointer(drv));
	}
	else {
		elog(DEBUG1, "json attr %s not found", attr);
	}

	pfree(DatumGetPointer(dattr));
	return rv;
}


Datum
jbu_getattr_obj(Datum jsonb, const char *attr)
{
	Datum rv = (Datum)NULL;
	Datum dattr = CStringGetTextDatum(attr);

	if (DatumGetBool(DirectFunctionCall2(jsonb_exists, jsonb, dattr))) {
		rv = DirectFunctionCall2(jsonb_object_field, jsonb, dattr);
	}
	else {
		elog(DEBUG1, "json attr %s not found", attr);
	}

	pfree(DatumGetPointer(dattr));
	return rv;
}


char *
jbu_getitem_str(Datum jsonb, int item)
{
	char *rv = NULL;
	Datum ditem = Int32GetDatum(item);
	Datum drv = DirectFunctionCall2(jsonb_array_element_text, jsonb, ditem);
	rv = TextDatumGetCString(drv);
	pfree(DatumGetPointer(drv));

	return rv;
}
