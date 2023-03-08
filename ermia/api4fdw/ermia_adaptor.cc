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
  ermia::Engine *fdw::ERMIAAdaptor::m_engine = nullptr;

  bool ERMIAAdaptor::m_initialized = false;

  void ERMIAAdaptor::Init()
  {
    if (m_engine == nullptr)
    {
      LOG(INFO) << "Start initializing ERMIA...";
      // FIXME: Hard-coded for now
      bool enable_commit_pipeline = true;
      ermia::config::log_dir = std::string("/dev/shm/khuang/ermia-log");
      ermia::config::tmpfs_dir = std::string("/dev/shm");
      ermia::config::log_redo_partitions = 32;
      ermia::config::recover_functor = new ermia::parallel_oid_replay(1);
      ermia::config::arena_size_mb = 4; //
      ermia::config::group_commit = true;
      ermia::config::group_commit_timeout = 5000;
      ermia::config::group_commit_queue_length = 3000;
      ermia::config::log_buffer_mb = 32;
      ermia::config::state = ermia::config::kStateForwardProcessing;

      // Make sure to enable ERMIA thread pool to create threads on all NUMA nodes
      ermia::config::threads = std::thread::hardware_concurrency();

      // Set worker_threads to create this number of commit queues;
      // for now take the max to make it one queue per hardware thread.
      ermia::config::worker_threads = std::thread::hardware_concurrency();
      ermia::config::dequeue_threads = 1;
      ermia::config::tls_alloc = false;
      ermia::config::threadpool = false;

      // TODO: do we need to pre-allocate str_arena here before initializing the engine?
      m_engine = new ermia::Engine();
    }
    m_initialized = true;
    LOG(INFO) << "Hello ERMIA!";
  }

  void ERMIAAdaptor::Destroy()
  {
    if (m_engine != nullptr)
    {
      LOG(INFO) << "Destroying ERMIA...";
      delete m_engine;
      m_engine = nullptr;
    }
    m_initialized = false;
    LOG(INFO) << "Bye ERMIA!";
  }
}
