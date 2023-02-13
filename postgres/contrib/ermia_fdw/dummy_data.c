/*
 * The dummy Foreign Data Wrapper allows you 
 * to test foreign data wrappers. 
 */

#include "postgres.h"
#include "access/sysattr.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h" 
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "commands/explain.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "optimizer/restrictinfo.h"

PG_MODULE_MAGIC;

extern Datum dummy_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(dummy_handler);

void		_PG_init(void);
void		_PG_fini(void);

/*
 * FDW functions declarations
 */

static void dummyGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);
static void dummyGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);
#if (PG_VERSION_NUM <= 90500)
static ForeignScan *dummyGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses);

#else
static ForeignScan *dummyGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses,
						Plan *outer_plan);
#endif
static void dummyBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *dummyIterateForeignScan(ForeignScanState *node);
static void dummyReScanForeignScan(ForeignScanState *node);
static void dummyEndForeignScan(ForeignScanState *node);


/*
 * FDW callback routines
 */
static void dummyAddForeignUpdateTargets(Query *parsetree,
								RangeTblEntry *target_rte,
								Relation target_relation);
static List *dummyPlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index);
static void dummyBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags);
static TupleTableSlot *dummyExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot);
static TupleTableSlot *dummyExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot);
static TupleTableSlot *dummyExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot);
static void dummyEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo);
static int	dummyIsForeignRelUpdatable(Relation rel);
static void dummyExplainForeignScan(ForeignScanState *node,
						   ExplainState *es);
static void dummyExplainForeignModify(ModifyTableState *mtstate,
							 ResultRelInfo *rinfo,
							 List *fdw_private,
							 int subplan_index,
							 ExplainState *es);
static bool dummyAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalpages);
static int dummyAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows,
							  double *totaldeadrows);

/* magic */
enum FdwScanPrivateIndex
{
  /* SQL statement to execute remotely (as a String node) */
  FdwScanPrivateSelectSql,
  /* Integer list of attribute numbers retrieved by the SELECT */
  FdwScanPrivateRetrievedAttrs
};
/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a postgres_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *    (NIL for a DELETE)
 * 3) Boolean flag showing if there's a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
  /* SQL statement to execute remotely (as a String node) */
  FdwModifyPrivateUpdateSql,
  /* Integer list of target attribute numbers for INSERT/UPDATE */
  FdwModifyPrivateTargetAttnums,
  /* has-returning flag (as an integer Value node) */
  FdwModifyPrivateHasReturning,
  /* Integer list of attribute numbers retrieved by RETURNING */
  FdwModifyPrivateRetrievedAttrs
};


void _PG_init() 
{
}

void _PG_fini()
{
}

Datum dummy_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	fdw_routine->GetForeignRelSize = dummyGetForeignRelSize;
	fdw_routine->GetForeignPaths = dummyGetForeignPaths;
	fdw_routine->GetForeignPlan = dummyGetForeignPlan;
	fdw_routine->ExplainForeignScan = dummyExplainForeignScan;
	fdw_routine->ExplainForeignModify = dummyExplainForeignModify;

	fdw_routine->BeginForeignScan = dummyBeginForeignScan;
	fdw_routine->IterateForeignScan = dummyIterateForeignScan;
	fdw_routine->ReScanForeignScan = dummyReScanForeignScan;
	fdw_routine->EndForeignScan = dummyEndForeignScan;

  /* insert support */
	fdw_routine->AddForeignUpdateTargets = dummyAddForeignUpdateTargets;

	fdw_routine->PlanForeignModify = dummyPlanForeignModify;
	fdw_routine->BeginForeignModify = dummyBeginForeignModify;
	fdw_routine->ExecForeignInsert = dummyExecForeignInsert;
	fdw_routine->ExecForeignUpdate = dummyExecForeignUpdate;
	fdw_routine->ExecForeignDelete = dummyExecForeignDelete;
	fdw_routine->EndForeignModify = dummyEndForeignModify;
	fdw_routine->IsForeignRelUpdatable = dummyIsForeignRelUpdatable;

	fdw_routine->AnalyzeForeignTable = dummyAnalyzeForeignTable;

	PG_RETURN_POINTER(fdw_routine);
}


/*
 * GetForeignRelSize
 *		set relation size estimates for a foreign table
 */
static void
dummyGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
		baserel-> rows = 0;
}

/*
 * GetForeignPaths
 *		create access path for a scan on the foreign table
 */
static void
dummyGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	Path	   *path;
#if (PG_VERSION_NUM < 90500)
	path = (Path *) create_foreignscan_path(root, baserel,
						baserel->rows,
						10,
						0,
						NIL,	
						NULL,	
						NULL);
#else
	path = (Path *) create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
						NULL,
#endif
						baserel->rows,
						10,
						0,
						NIL,
						NULL,
						NULL,
						NIL);
#endif
  add_path(baserel, path);
}

/*
 * GetForeignPlan
 *	create a ForeignScan plan node 
 */
#if (PG_VERSION_NUM <= 90500)
static ForeignScan *
dummyGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	Index		scan_relid = baserel->relid;
  Datum    blob = 0;
  Const    *blob2 = makeConst(INTERNALOID, 0, 0,
                 sizeof(blob),
                 blob,
                 false, false);
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	return make_foreignscan(tlist,
			scan_clauses,
			scan_relid,
			scan_clauses,		
			(void *)blob2);
}
#else
static ForeignScan *
dummyGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses,
						Plan *outer_plan)
{
	Index		scan_relid = baserel->relid;
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
			scan_clauses,
			scan_relid,
			scan_clauses,
			NIL,
			NIL,
			NIL,
			outer_plan);
}
#endif
/*
 * ExplainForeignScan
 *   no extra info explain plan
 */
/*
static void
dummyExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
}

*/
/*
 * BeginForeignScan
 *   called during executor startup. perform any initialization 
 *   needed, but not start the actual scan. 
 */

static void
dummyBeginForeignScan(ForeignScanState *node, int eflags)
{
}



/*
 * IterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 *   Fetch one row from the foreign source, returning it in a tuple table slot 
 *    (the node's ScanTupleSlot should be used for this purpose). 
 *  Return NULL if no more rows are available. 
 */
static TupleTableSlot *
dummyIterateForeignScan(ForeignScanState *node)
{
	return NULL;
}

/*
 * ReScanForeignScan
 *		Restart the scan from the beginning
 */
static void
dummyReScanForeignScan(ForeignScanState *node)
{
}

/*
 *EndForeignScan
 *	End the scan and release resources. 
 */
static void
dummyEndForeignScan(ForeignScanState *node)
{
}


/*
 * postgresAddForeignUpdateTargets
 *    Add resjunk column(s) needed for update/delete on a foreign table
 */
static void 
dummyAddForeignUpdateTargets(Query *parsetree,
								RangeTblEntry *target_rte,
								Relation target_relation)
{
 Var      *var;
  const char *attrname;
  TargetEntry *tle;

/*
 * In postgres_fdw, what we need is the ctid, same as for a regular table.
 */

  /* Make a Var representing the desired value */
  var = makeVar(parsetree->resultRelation,
          SelfItemPointerAttributeNumber,
          TIDOID,
          -1,
          InvalidOid,
          0);

  /* Wrap it in a resjunk TLE with the right name ... */
  attrname = "ctid";

  tle = makeTargetEntry((Expr *) var,
              list_length(parsetree->targetList) + 1,
              pstrdup(attrname),
              true);

  /* ... and add it to the query's targetlist */
  parsetree->targetList = lappend(parsetree->targetList, tle);
}





/* 
#include "postgres.h"

#include "postgres_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

*/

/*
 * dummyPlanForeignModify
 *		Plan an insert/update/delete operation on a foreign table
 *
 * Note: currently, the plan tree generated for UPDATE/DELETE will always
 * include a ForeignScan that retrieves ctids (using SELECT FOR UPDATE)
 * and then the ModifyTable node will have to execute individual remote
 * UPDATE/DELETE commands.  If there are no local conditions or joins
 * needed, it'd be better to let the scan node do UPDATE/DELETE RETURNING
 * and then do nothing at ModifyTable.  Room for future optimization ...
 */
static List *
dummyPlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index)
{
/*
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
*/
	List	   *targetAttrs = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;

	StringInfoData sql;
	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
//	rel = heap_open(rte->relid, NoLock);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
/*
	if (operation == CMD_INSERT)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		Bitmapset  *tmpset = bms_copy(rte->modifiedCols);
		AttrNumber	col;

		while ((col = bms_first_member(tmpset)) >= 0)
		{
			col += FirstLowInvalidHeapAttributeNumber;
			if (col <= InvalidAttrNumber)		// shouldn't happen 
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, col);
		}
	}
*/

	/*
	 * Extract the relevant RETURNING list if any.
	 */
/*
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);
*/

	/*
	 * Construct the SQL command string.
	 */
/*
	switch (operation)
	{
		case CMD_INSERT:
			deparseInsertSql(&sql, root, resultRelation, rel,
							 targetAttrs, returningList,
							 &retrieved_attrs);
			break;
		case CMD_UPDATE:
			deparseUpdateSql(&sql, root, resultRelation, rel,
							 targetAttrs, returningList,
							 &retrieved_attrs);
			break;
		case CMD_DELETE:
			deparseDeleteSql(&sql, root, resultRelation, rel,
							 returningList,
							 &retrieved_attrs);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	heap_close(rel, NoLock);
*/

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make4(makeString(sql.data),
					  targetAttrs,
					  makeInteger((returningList != NIL)),
					  retrieved_attrs);
}

/*
 * dummyBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
static void
dummyBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags)
{
		return;
}

/*
 * dummyExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
dummyExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	return NULL;
}

/*
 * dummyExecForeignUpdate
 *		Update one row in a foreign table
 */
static TupleTableSlot *
dummyExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	return NULL;
}

/*
 * dummyExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *
dummyExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	return NULL;
}

/*
 * dummyEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
dummyEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	return;
}

/*
 * dummyIsForeignRelUpdatable
 *  Assume table is updatable regardless of settings.
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
dummyIsForeignRelUpdatable(Relation rel)
{
	/* updatable is INSERT, UPDATE and DELETE.
	 */
	return (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE) ;
}

/*
 * dummyExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
dummyExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
/*
	List	   *fdw_private;
	char	   *sql;

	if (es->verbose)
	{
		fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Dummy SQL", sql, es);
	}
*/
  
}

/*
 * dummyExplainForeignModify
 *		Produce extra output for EXPLAIN of a ModifyTable on a foreign table
 */
static void
dummyExplainForeignModify(ModifyTableState *mtstate,
							 ResultRelInfo *rinfo,
							 List *fdw_private,
							 int subplan_index,
							 ExplainState *es)
{
	if (es->verbose)
	{
		char	   *sql = strVal(list_nth(fdw_private,
										  FdwModifyPrivateUpdateSql));

		ExplainPropertyText("Dummy SQL", sql, es);
	}
}


/*
 * dummyAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
dummyAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalpages)
{
  *func = dummyAcquireSampleRowsFunc ;
	return false;
}

/*
 * Acquire a random sample of rows
 */
static int
dummyAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows,
							  double *totaldeadrows)
{
  
  totalrows = 0;
  totaldeadrows = 0;
	return 0;
}

