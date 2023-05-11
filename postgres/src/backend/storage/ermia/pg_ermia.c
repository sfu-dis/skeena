#include "pg_ermia.h"
#include "api4fdw/ermia_api.h"

/*
 * Initializes ERMIA engine.
 */
void ERMIAInit(void)
{
	AdaptorInit();
}    

/*
 * Shutdown the ERMIA engine.
 */
void ERMIATerm(void)
{
	if (!AdaptorExists())
	{
		return;
	}
	AdaptorDestroy();
}

/*
 * Create a table in ERMIA.
 */
void ERMIACreateTable(CreateForeignTableStmt* stmt, TransactionId tid)
{
	AdaptorCreateTable(stmt->base.relation->schemaname, stmt->base.relation->relname);
}
