#ifndef _JSONBUTILS_H_
#define _JSONBUTILS_H_

#include "postgres.h"

Datum jbu_create(const char *s);
bool jbu_is_type(Datum jsonb, const char *type);
int jbu_array_len(Datum jsonb);
char *jbu_getattr_str(Datum jsonb, const char *attr);
Datum jbu_getattr_obj(Datum jsonb, const char *attr);
char *jbu_getitem_str(Datum jsonb, int item);

#endif
