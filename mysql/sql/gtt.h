#ifndef GTT_INCLUDED
#define GTT_INCLUDED

#include <functional>
#include "dbcore/sm-alloc.h"
#include "dbcore/epoch.h"
#include "masstree/masstree_btree_noxid.h"
#include "sql/mysqld.h"
#include <endian.h>
#include <pthread.h>

#define MAX_IDS_LIMIT 128
#define CROSS_TRX_INHERIT 1
#define CROSS_TRX_ORIGIN 2
#define CROSS_TRX_ABORT 3

#define GTT_BYPASS_RETURN(obj)    \
  do {                            \
    if(bypass) return obj;        \
  } while(0)

#define GTT_BYPASS                \
  do {                            \
    if(bypass) return;            \
  } while(0)

struct handlerton;

class GlobalTransactionTable {
  private:
    // Reader-writer lock that protects the list of all index trees
    static pthread_rwlock_t lock; 
   public:

    // LSN index structure
    struct Index {
      // ERMIA begin LSN of the very first insert
      uint64_t min_elsn;

      // Max number of keys in the masstree
      uint64_t capacity;

      // Number of keys in the masstree
      uint64_t num_entries;

      // Lock protecting states
      std::mutex lock;

      // Masstree that maps ERMIA LSNs to list of InnoDB trx IDs
      mtt::ConcurrentMasstree mtree;

      Index(uint64_t cap, uint64_t lsn) : min_elsn(lsn), capacity(cap), num_entries(0) {
        gtt_tree_count += 1;
      }
      ~Index() {}

      inline bool allow_insert() { return num_entries < capacity; }
      inline bool is_empty() { return num_entries == 0; }
    };

   private:
    // List of InnoDB transaction IDs (i.e., the "payload" of mtree)
    struct trx_id_list {
      std::vector<uint64_t> entries;
      trx_id_list() {}
      ~trx_id_list() {}
    };

    class GTTLockGuard {
     private:
      bool released;
     public:
      GTTLockGuard(bool shared) {
        if (shared) {
          int ret = pthread_rwlock_rdlock(&lock);
          ALWAYS_ASSERT(ret == 0);
        } else {
          int ret = pthread_rwlock_wrlock(&lock);
          ALWAYS_ASSERT(ret == 0);
        }
        released = false;
      }
      ~GTTLockGuard() {
        int ret = pthread_rwlock_unlock(&lock);
        ALWAYS_ASSERT(ret == 0);
      }
      void release() {
        int ret = pthread_rwlock_unlock(&lock);
        ALWAYS_ASSERT(ret == 0);
        released = true;
      }
    };

    // List of all trees, naturally sorted by min_elsn
    // (inclusive, [i]'s min_elsn may equal to [i+1]'s).
    // Only the last index allows new inserts.
    std::vector<Index *> commit_indexes;

    ermia::epoch_mgr epoch;

    // The knob whether we bypass cross-container txn
    bool bypass = false;

    // Obtain the masstree index covering [elsn]
    // @elsn: target key covered by index
    Index *get_index(uint64_t elsn);
   public:

    // Capacity of each tree (to be passed as parameter when creating a new
    // tree)
    uint64_t tree_capacity;

    // Trigger tree recycling after we have this many trees
    uint64_t recycle_threshold;

    // Whether tree recycling is enabled
    bool do_recycle = false;

  public:
    bool commit_check(uint64_t pre_ermia_lsn, uint64_t inno_trx_id);
    GlobalTransactionTable(uint32_t cap);
    ~GlobalTransactionTable();

    // Find a proper innodb transaction ID based on a given ERMIA LSN
    // @lsn: ERMIA LSN
    // @orig_trx_id
    // @fn: callback function
    uint64_t get_innodb_trx_id(uint64_t lsn, std::function<uint64_t(void)> fn);

    // Clean up trees that are no longer needed
    // @min_active_elsn: minimum ERMIA LSN across all transactions (THDs)
    void recycle(uint64_t min_active_elsn);


    // Helper functions
    inline void setThreshold(uint64_t threshold) { recycle_threshold = threshold; }
    inline uint64_t get_threshold() { return recycle_threshold; }
    inline void setBypass(bool byp) { bypass = byp; }
    inline bool is_bypass() { return bypass; }
    inline void setCapacity(uint64_t cap) { tree_capacity = cap; }
    inline uint64_t get_capacity() { return tree_capacity; }
    inline void setDoRecycle(bool doit) { do_recycle = doit; }
    inline bool get_do_recycle() { return do_recycle; }
    inline void epoch_init_once() {
      if (!epoch.thread_initialized()) {
          epoch.thread_init();
      }
    }
};

class GTTFetchCallback : public mtt::NoXIDSearchRangeCallback {
public:
    uintptr_t result = 0;
    GTTFetchCallback() : NoXIDSearchRangeCallback(nullptr)  {}
    inline bool found() { return result ? true: false; }

private:
    typename mtt::ConcurrentMasstree::string_type skip;
    bool invoke(const typename mtt::ConcurrentMasstree::string_type &k, uintptr_t ptr, uint64_t version) {
        MARK_REFERENCED(k);
        MARK_REFERENCED(version);
        ALWAYS_ASSERT(ptr);
        result = ptr;
        return false; // only want one record with key > k
    }
};

class GTTDebugCallback : public mtt::NoXIDSearchRangeCallback {
public:
    GTTDebugCallback() : NoXIDSearchRangeCallback(nullptr)  {}
    inline bool found() { return true;}
private:
    struct trx_id_list {
      std::vector<uint64_t> entries;
      trx_id_list() {}
      ~trx_id_list() {}
    };

    typename mtt::ConcurrentMasstree::string_type skip;
    bool invoke(const typename mtt::ConcurrentMasstree::string_type &k, uintptr_t ptr, uint64_t version) {
        MARK_REFERENCED(k);
        MARK_REFERENCED(version);
        if (!ptr) {
            return false;
        }
        auto list = reinterpret_cast<trx_id_list *>(ptr);
        printf("Key(LSN): 0x%lx, Value: ", (uint64_t)k.data());
        for (auto item : list->entries) {
            printf("%lu->", item);
        }
        printf("END\n");
        return true;
    }
};

#endif // GTT_INCLUDED
