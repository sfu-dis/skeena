/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include "bench.h"
#include "ycsb.h"

#ifndef ADV_COROUTINE

extern uint g_reps_per_tx;
extern uint g_rmw_additional_reads;
extern YcsbWorkload ycsb_workload;

class ycsb_sequential_worker : public ycsb_base_worker {
 public:
  ycsb_sequential_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                         const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                         spin_barrier *barrier_a, spin_barrier *barrier_b)
    : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
  }

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;
    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent() || ycsb_workload.scan_percent()) {
      LOG(FATAL) << "Not implemented";
    }

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, TxnRead));
    }

    if (ycsb_workload.rmw_percent()) {
      w.push_back(workload_desc("RMW", double(ycsb_workload.rmw_percent()) / 100.0, TxnRMW));
    }

    return w;
  }

  static rc_t TxnRead(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read(); }
  static rc_t TxnRMW(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_rmw(); }

  // Read transaction using traditional sequential execution
  rc_t txn_read() {
    ermia::transaction *txn = nullptr;
    txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());

    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      // TODO: add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif
      memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
    }
    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  // Read-modify-write transaction. Sequential execution only
  rc_t txn_rmw() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      // TODO: add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      ermia::OID oid = ermia::INVALID_OID;
      table_index->GetRecord(txn, rc, k, v, &oid);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      LOG_IF(FATAL, rc._val != RC_TRUE);
      ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif

      ALWAYS_ASSERT(v.size() == sizeof(ycsb_kv::value));
      memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());

      // Re-initialize the value structure to use my own allocated memory -
      // DoTupleRead will change v.p to the object's data area to avoid memory
      // copy (in the read op we just did).
      new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
      new (v.data()) ycsb_kv::value("a");
      TryCatch(table_index->UpdateRecord(txn, k, v));  // Modify-write
    }

    for (uint i = 0; i < g_rmw_additional_reads; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));

      // TODO: add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif
      ALWAYS_ASSERT(v.size() == sizeof(ycsb_kv::value));
      memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());
    }
    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

 private:
  std::vector<ermia::varstr *> keys;
  std::vector<ermia::varstr *> values;
};

void ycsb_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_sequential_worker> r(db);
  r.run();
}

#endif  // ADV_COROUTINE
