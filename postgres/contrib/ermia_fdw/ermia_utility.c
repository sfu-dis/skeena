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

#include <sys/stat.h>

#include "ermia_fdw.h"
// #include "server/kv_storage.h"

#include "foreign/foreign.h"
#include "miscadmin.h"
#include "commands/event_trigger.h"
#include "tcop/utility.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "commands/defrem.h"
#include "utils/rel.h"
#include "commands/copy.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "executor/executor.h"
#include "utils/typcache.h"
#include "commands/dbcommands.h"
#include "access/table.h"

#define ERMIAFDWNAME "ermia_fdw"

/* Forward Declaration */
void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(ermia_ddl_event_end_trigger);

/* Checks if a directory exists for the given directory name. */
static bool ERMIADirectoryExists(StringInfo directoryName) {
  bool directoryExists = true;
  struct stat directoryStat;
  if (stat(directoryName->data, &directoryStat) == 0) {
    /* file already exists; check that it is a directory */
    if (!S_ISDIR(directoryStat.st_mode)) {
      ereport(ERROR,
              errmsg("\"%s\" is not a directory", directoryName->data),
              errhint("You need to remove or rename the file \"%s\".", directoryName->data));
    }
  } else {
    if (errno == ENOENT) {
      directoryExists = false;
    } else {
      ereport(ERROR,
              errcode_for_file_access(),
              errmsg("could not stat directory \"%s\": %m", directoryName->data));
    }
  }

  return directoryExists;
}

/*
 * Creates the directory (and parent directories, if needed)
 * used to store automatically managed ermia_fdw files. The path to
 * the directory is $PGDATA/ermia_fdw/{databaseOid}.
 */
static void ERMIACreateDatabaseDirectory(Oid databaseOid) {
  StringInfo directoryPath = makeStringInfo();
  appendStringInfo(directoryPath, "%s/%s", DataDir, ERMIAFDWNAME);
  if (!ERMIADirectoryExists(directoryPath)) {
    elog(ERROR, "ermia directory does not exist");
    if (mkdir(directoryPath->data, S_IRWXU) != 0) {
      ereport(ERROR, errcode_for_file_access(),
                      errmsg("could not create directory \"%s\": %m",
                            directoryPath->data));
    }
  }

  StringInfo databaseDirectoryPath = makeStringInfo();
  appendStringInfo(databaseDirectoryPath, "%s/%s/%u", DataDir, ERMIAFDWNAME, databaseOid);
  if (!ERMIADirectoryExists(databaseDirectoryPath)) {
    elog(ERROR, "ermia database directory does not exist");
    if (mkdir(databaseDirectoryPath->data, S_IRWXU) != 0) {
      ereport(ERROR, errcode_for_file_access(),
                      errmsg("could not create directory \"%s\": %m",
                            databaseDirectoryPath->data));
    }
  }
}

/*
 * Checks if the given foreign server belongs to ermia_fdw. If it
 * does, the function returns true. Otherwise, it returns false.
 */
static bool ERMIAServer(ForeignServer* server) {
  char* fdwName = GetForeignDataWrapper(server->fdwid)->fdwname;
  return strncmp(fdwName, ERMIAFDWNAME, NAMEDATALEN) == 0;
}


Datum ermia_ddl_event_end_trigger(PG_FUNCTION_ARGS) {
	/* error if event trigger manager did not call this function */
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) {
		ereport(ERROR, errmsg("trigger not fired by event trigger manager"));
	}

	EventTriggerData* triggerData = (EventTriggerData*) fcinfo->context;
	Node* parseTree = triggerData->parsetree;

	if (nodeTag(parseTree) == T_CreateForeignServerStmt) {
		CreateForeignServerStmt* serverStmt = (CreateForeignServerStmt*) parseTree;
		if (strncmp(serverStmt->fdwname, "ermia_fdw", NAMEDATALEN) == 0) {
			elog(ERROR, "creating an instance of ERMIA");
			ERMIACreateDatabaseDirectory(MyDatabaseId);
		}
	} else if (nodeTag(parseTree) == T_CreateForeignTableStmt) {
		CreateForeignTableStmt* tableStmt = (CreateForeignTableStmt*) parseTree;
		ForeignServer* server = GetForeignServerByName(tableStmt->servername, false);
		if (ERMIAServer(server)) {
			elog(ERROR, "creating a table in ERMIA");
		}
	}

	PG_RETURN_NULL();
}

/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void _PG_init(void) {}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void _PG_fini(void) {}