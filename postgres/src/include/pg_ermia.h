#ifndef PG_ERMIA_H
#define PG_ERMIA_H

#include "postgres.h"

#include "nodes/parsenodes.h"

/*
 * Initializes ERMIA engine.
 */
extern void ERMIAInit(void);

/*
 * Shutdown the ERMIA engine.
 */
extern void ERMIATerm(void);

/*
 * Create a table in ERMIA.
 */
extern void ERMIACreateTable(CreateForeignTableStmt* stmt, TransactionId tid);

#endif // PG_ERMIA_H
