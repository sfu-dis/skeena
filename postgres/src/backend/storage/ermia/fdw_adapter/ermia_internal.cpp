#include "ermia_internal.h"

void ERMIAAdaptorInit()
{
  ERMIAAdaptor::Init();
}

void ERMIAAdaptorDestroy()
{
  ERMIAAdaptor::Destroy();
}

bool ERMIAAdaptorExists()
{
  return true;
}

bool ERMIAAdaptor::m_initialized = false;

void ERMIAAdaptor::Init()
{
  elog(LOG, "Initialize an instance of ERMIA");

  if (m_initialized)
  {
    elog(FATAL, "Double attempt to initialize ERMIA engine, it is already initialized");
  }



  m_initialized = true;
  elog(LOG, "!!!Hello ERMIA!!!");
}

void ERMIAAdaptor::Destroy()
{
  elog(LOG, "!!!Bye ERMIA!!!");
}
