#include "ermia_adaptor.h"

void ERMIAAdaptorInit()
{
  fdw::ERMIAAdaptor::Init();
}

void ERMIAAdaptorDestroy()
{
  fdw::ERMIAAdaptor::Destroy();
}

bool ERMIAAdaptorExists()
{
  return fdw::ERMIAAdaptor::m_initialized;
}

namespace fdw
{
  bool ERMIAAdaptor::m_initialized = false;

  void ERMIAAdaptor::Init()
  {
    if (m_initialized)
    {
      LOG(FATAL) << "ERMIA engine already exists.";
    }

    ermia::Engine::MockInit();

    m_initialized = true;
  }

  void ERMIAAdaptor::Destroy()
  {
    ermia::Engine::MockDestroy();
  }
}
