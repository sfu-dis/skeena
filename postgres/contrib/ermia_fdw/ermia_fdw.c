/* Copyright (c) 2019, 2020, Simon Fraser University. All rights reserved.
  Copyright (c) 2004, 2017, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "ermia_fdw.h"
#include "ermia_api.h"

#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"

#include "access/detoast.h"
#include "access/heaptoast.h"
#include "catalog/pg_operator.h"
#include "utils/syscache.h"
#include "access/table.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ermia_fdw_handler);

/*
 * FDW functions declarations
 */

static void ermiaGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);
static void ermiaGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);
static ForeignScan *ermiaGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses,
						Plan *outer_plan);
static void ermiaBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *ermiaIterateForeignScan(ForeignScanState *node);
static void ermiaReScanForeignScan(ForeignScanState *node);
static void ermiaEndForeignScan(ForeignScanState *node);

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

Datum ermia_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	fdw_routine->GetForeignRelSize = ermiaGetForeignRelSize;
	fdw_routine->GetForeignPaths = ermiaGetForeignPaths;
	fdw_routine->GetForeignPlan = ermiaGetForeignPlan;
	fdw_routine->BeginForeignScan = ermiaBeginForeignScan;
	fdw_routine->IterateForeignScan = ermiaIterateForeignScan;
	fdw_routine->ReScanForeignScan = ermiaReScanForeignScan;
	fdw_routine->EndForeignScan = ermiaEndForeignScan;

	/*
	 * Remaining functions are optional. Set the pointer to NULL for any that are not provided.
	 */

	/* Functions for updating foreign tables */
	fdw_routine->AddForeignUpdateTargets = NULL;
	fdw_routine->PlanForeignModify = NULL;
	fdw_routine->BeginForeignModify = NULL;
	fdw_routine->ExecForeignInsert = NULL;
	fdw_routine->ExecForeignUpdate = NULL;
	fdw_routine->ExecForeignDelete = NULL;
	fdw_routine->EndForeignModify = NULL;
	fdw_routine->IsForeignRelUpdatable = NULL;

	/* Support functions for EXPLAIN */
	fdw_routine->ExplainForeignScan = NULL;
	fdw_routine->ExplainForeignModify = NULL;

	/* Support functions for ANALYZE */
	fdw_routine->AnalyzeForeignTable = NULL;

	PG_RETURN_POINTER(fdw_routine);
}

/*
 * GetForeignRelSize
 *		set relation size estimates for a foreign table
 */
static void
ermiaGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	// TODO
}

/*
 * GetForeignPaths
 *		create access path for a scan on the foreign table
 */
static void
ermiaGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	// TODO
}

/*
 * GetForeignPlan
 *	create a ForeignScan plan node
 */
static ForeignScan *
ermiaGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses,
						Plan *outer_plan)
{
	// TODO
	return NULL;
}

/*
 * BeginForeignScan
 *   called during executor startup. perform any initialization
 *   needed, but not start the actual scan.
 */

static void
ermiaBeginForeignScan(ForeignScanState *node, int eflags)
{
	// TODO
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
ermiaIterateForeignScan(ForeignScanState *node)
{
	// TODO
	return NULL;
}

/*
 * ReScanForeignScan
 *		Restart the scan from the beginning
 */
static void
ermiaReScanForeignScan(ForeignScanState *node)
{
	// TODO
}

/*
 *EndForeignScan
 *	End the scan and release resources.
 */
static void
ermiaEndForeignScan(ForeignScanState *node)
{
	// TODO
}
