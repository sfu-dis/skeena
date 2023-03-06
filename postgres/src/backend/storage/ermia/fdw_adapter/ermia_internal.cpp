#include "ermia_internal.h"

void ERMIAAdaptorInit() {
  ERMIAAdaptor::Init();
}

void ERMIAAdaptor::Init()
{
  elog(LOG, "Hello ERMIAAdaptor::Init()");
}
