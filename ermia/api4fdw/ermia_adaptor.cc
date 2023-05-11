#include "ermia_adaptor.h"

void AdaptorInit() { fdw::Adaptor::Init(); }

void AdaptorDestroy() { fdw::Adaptor::Destroy(); }

bool AdaptorExists() { return fdw::Adaptor::Exists(); }

void AdaptorCreateTable(const char *schemaname, const char *relname) {
  fdw::Adaptor::GetInstance()->CreateERMIATable(schemaname, relname);
}

namespace fdw {
Adaptor *fdw::Adaptor::m_adaptor = nullptr;
ermia::Engine *fdw::Adaptor::m_engine = nullptr;
bool Adaptor::m_initialized = false;

void Adaptor::Init() {
  if (m_adaptor == nullptr) {
    m_adaptor = new Adaptor();
  }
}

void Adaptor::Destroy() {
  if (m_adaptor != nullptr) {
    DestroyERMIA();
    LOG(INFO) << "Destroying Adaptor...";
    delete m_adaptor;
    m_adaptor = nullptr;
  }
  LOG(INFO) << "Bye Adaptor!";
}

Adaptor *Adaptor::GetInstance() {
  Init();
  return m_adaptor;
}

bool Adaptor::Exists() {
  return m_initialized;
}

void Adaptor::InitERMIA() {
  if (m_engine == nullptr) {
    LOG(INFO) << "Start initializing ERMIA...";
    // FIXME: Hard-coded for now
    bool enable_commit_pipeline = true;
    ermia::config::log_dir = std::string("/dev/shm/khuang/ermia-log");
    ermia::config::tmpfs_dir = std::string("/dev/shm");
    ermia::config::log_redo_partitions = 32;
    ermia::config::recover_functor = new ermia::parallel_oid_replay(1);
    ermia::config::arena_size_mb = 4;  //
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
    LOG(INFO) << "Hello ERMIA!";
  }
  m_initialized = true;
}

void Adaptor::DestroyERMIA() {
  if (m_engine != nullptr) {
    LOG(INFO) << "Destroying ERMIA...";
    delete m_engine;
    m_engine = nullptr;
  }
  m_initialized = false;
  LOG(INFO) << "Bye ERMIA!";
}

ermia::Engine *Adaptor::GetERMIA() {
  InitERMIA();
  return m_engine;
}

void Adaptor::CreateERMIATable(const char *schemaname, const char *relname) {
  static const uint32_t kMaxName = 512;
  char full_table_name[kMaxName];
  char full_index_name[kMaxName];

  get_full_table_name(schemaname, relname, full_table_name);
  get_full_index_name(schemaname, relname, full_index_name);

  LOG(INFO) << "Starts creating an ERMIA table: " << full_table_name;
  m_engine->CreateTable(full_table_name);
  LOG(INFO) << "Finishes creating an ERMIA table: " << full_table_name;

  LOG(INFO) << "Starts creating primary index: " << full_index_name;
  m_engine->CreateMasstreePrimaryIndex(full_table_name, full_index_name);
  LOG(INFO) << "Finishes creating primary index: " << full_index_name;
}

void Adaptor::get_full_table_name(const char *schemaname, const char *relname, char *buf) {
  uint16_t idx = 0;
  uint16_t l_schemaname = strlen(schemaname);
  uint16_t l_relname = strlen(relname);

  memcpy(buf, schemaname, l_schemaname);
  idx += l_schemaname;

  buf[idx] = '/';
  ++idx;

  memcpy(buf + idx, relname, l_relname);
  idx += l_relname;

  buf[idx] = '\0';
}

void Adaptor::get_full_index_name(const char *schemaname, const char *relname, char *buf) {
  uint16_t idx = 0;
  uint16_t l_schemaname = strlen(schemaname);
  uint16_t l_relname = strlen(relname);
  char primary[8] = "Primary";

  memcpy(buf, schemaname, l_schemaname);
  idx += l_schemaname;

  buf[idx] = '/';
  ++idx;

  memcpy(buf + idx, relname, l_relname);
  idx += l_relname;

  buf[idx] = '/';
  ++idx;

  strcpy(buf + idx, primary);
}
}  // namespace fdw
