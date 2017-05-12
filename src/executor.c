/*-------------------------------------------------------------------------
 *
 * code "borrowed" from pglogical
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor.h"

#include "catalog/pg_type.h"

#include "commands/trigger.h"

#include "nodes/nodeFuncs.h"

#include "optimizer/planner.h"

#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"

#include "tcop/utility.h"

#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


EState *
create_estate_for_relation(Relation rel, bool hasTriggers)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	RangeTblEntry *rte;


	/* Dummy range table entry needed by executor. */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, 0);

	/* Initialize executor state. */
	estate = CreateExecutorState();
	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_range_table = list_make1(rte);

	if (hasTriggers)
		resultRelInfo->ri_TrigDesc = CopyTriggerDesc(rel->trigdesc);

	if (resultRelInfo->ri_TrigDesc)
	{
		int			n = resultRelInfo->ri_TrigDesc->numtriggers;

		resultRelInfo->ri_TrigFunctions = (FmgrInfo *)
			palloc0(n * sizeof(FmgrInfo));
#if PG_VERSION_NUM >= 100000
		resultRelInfo->ri_TrigWhenExprs = (ExprState **)
			palloc0(n * sizeof(ExprState *));
#else
		resultRelInfo->ri_TrigWhenExprs = (List **)
			palloc0(n * sizeof(List *));
#endif

		/* Triggers might need a slot */
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);
	}
	else
	{
		resultRelInfo->ri_TrigFunctions = NULL;
		resultRelInfo->ri_TrigWhenExprs = NULL;
	}

	return estate;
}

ExprContext *
prepare_per_tuple_econtext(EState *estate, TupleDesc tupdesc)
{
	ExprContext	   *econtext;
	MemoryContext	oldContext;

	econtext = GetPerTupleExprContext(estate);

	oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
	econtext->ecxt_scantuple = ExecInitExtraTupleSlot(estate);
	MemoryContextSwitchTo(oldContext);

	ExecSetSlotDescriptor(econtext->ecxt_scantuple, tupdesc);

	return econtext;
}

ExprState *
prepare_row_filter(Node *row_filter)
{
	ExprState  *exprstate;
	Expr	   *expr;
	Oid			exprtype;

	exprtype = exprType(row_filter);
	expr = (Expr *) coerce_to_target_type(NULL,	/* no UNKNOWN params here */
										  row_filter, exprtype,
										  BOOLOID, -1,
										  COERCION_ASSIGNMENT,
										  COERCE_IMPLICIT_CAST,
										  -1);

	/* This should never happen but just to be sure. */
	if (expr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot cast the row_filter to boolean"),
			   errhint("You will need to rewrite the row_filter.")));

	expr = expression_planner(expr);
	exprstate = ExecInitExpr(expr, NULL);

	return exprstate;
}


/*
 * error context callback for parse failure pglogical_replication_set_add_table()
 */
static void
add_table_parser_error_callback(void *arg)
{
	const char *row_filter_str = (const char *) arg;

	errcontext("invalid row_filter expression \"%s\"", row_filter_str);

	/*
	 * Currently we just suppress any syntax error position report, rather
	 * than transforming to an "internal query" error.  It's unlikely that a
	 * type name is complex enough to need positioning.
	 */
	errposition(0);
}


Node *
parse_row_filter(Relation rel, char *row_filter_str)
{
	Node	   *row_filter = NULL;
	List	   *raw_parsetree_list;
	SelectStmt *stmt;
	ResTarget  *restarget;
	ParseState *pstate;
	char	   *nspname;
	char	   *relname;
	RangeTblEntry *rte;
	StringInfoData buf;
	ErrorContextCallback myerrcontext;

	nspname = get_namespace_name(RelationGetNamespace(rel));
	relname = RelationGetRelationName(rel);

	/*
	 * Build fake query which includes the expression so that we can
	 * pass it to the parser.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT %s FROM %s", row_filter_str,
					 quote_qualified_identifier(nspname, relname));

	/* Parse it, providing proper error context. */
	myerrcontext.callback = add_table_parser_error_callback;
	myerrcontext.arg = (void *) row_filter_str;
	myerrcontext.previous = error_context_stack;
	error_context_stack = &myerrcontext;

	raw_parsetree_list = pg_parse_query(buf.data);

	error_context_stack = myerrcontext.previous;

	/* Validate the output from the parser. */
	if (list_length(raw_parsetree_list) != 1)
		goto fail;
#if PG_VERSION_NUM >= 100000
	stmt = (SelectStmt *) linitial_node(RawStmt, raw_parsetree_list)->stmt;
#else
	stmt = (SelectStmt *) linitial(raw_parsetree_list);
#endif
	if (stmt == NULL ||
		!IsA(stmt, SelectStmt) ||
		stmt->distinctClause != NIL ||
		stmt->intoClause != NULL ||
		stmt->whereClause != NULL ||
		stmt->groupClause != NIL ||
		stmt->havingClause != NULL ||
		stmt->windowClause != NIL ||
		stmt->valuesLists != NIL ||
		stmt->sortClause != NIL ||
		stmt->limitOffset != NULL ||
		stmt->limitCount != NULL ||
		stmt->lockingClause != NIL ||
		stmt->withClause != NULL ||
		stmt->op != SETOP_NONE)
		goto fail;
	if (list_length(stmt->targetList) != 1)
		goto fail;
	restarget = (ResTarget *) linitial(stmt->targetList);
	if (restarget == NULL ||
		!IsA(restarget, ResTarget) ||
		restarget->name != NULL ||
		restarget->indirection != NIL ||
		restarget->val == NULL)
		goto fail;

	row_filter = restarget->val;

	/*
	 * Create a dummy ParseState and insert the target relation as its sole
	 * rangetable entry.  We need a ParseState for transformExpr.
	 */
	pstate = make_parsestate(NULL);
	rte = addRangeTableEntryForRelation(pstate,
										rel,
										NULL,
										false,
										true);
	addRTEtoQuery(pstate, rte, true, true, true);
	/*
	 * Transform the expression and check it follows limits of row_filter
	 * which are same as those of CHECK constraint so we can use the builtin
	 * checks for that.
	 *
	 * TODO: make the errors look more informative (currently they will
	 * complain about CHECK constraint. (Possibly add context?)
	 */
	row_filter = transformExpr(pstate, row_filter, EXPR_KIND_CHECK_CONSTRAINT);
	row_filter = coerce_to_boolean(pstate, row_filter, "row_filter");
	assign_expr_collations(pstate, row_filter);
	if (list_length(pstate->p_rtable) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("only table \"%s\" can be referenced in row_filter",
						relname)));
	pfree(buf.data);

	return row_filter;

fail:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid row_filter expression \"%s\"", row_filter_str)));
	return NULL;	/* keep compiler quiet */
}
