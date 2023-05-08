#include "pg_ermia.h"
#include "api4fdw/ermia_api.h"

/*
 * Initializes ERMIA engine.
 */
void InitERMIA(void)
{
	ERMIAAdaptorInit();
}    

/*
 * Shutdown the ERMIA engine.
 */
void TermERMIA(void)
{
	if (!ERMIAAdaptorExists())
	{
		return;
	}
	ERMIAAdaptorDestroy();
}

/*
 * Create a table in ERMIA.
 */
void CreateERMIATable(void)
{
	ERMIAAdaptorCreateTable();
}
