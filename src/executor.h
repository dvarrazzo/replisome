#ifndef _EXECUTOR_H_
#define _EXECUTOR_H_

#include "executor/executor.h"

EState *create_estate_for_relation(Relation rel, bool hasTriggers);
ExprContext *prepare_per_tuple_econtext(EState *estate, TupleDesc tupdesc);
ExprState *prepare_row_filter(Node *row_filter);

Node *parse_row_filter(Relation rel, char *row_filter_str);
bool validate_row_filter(char *row_filter_str);

#endif
