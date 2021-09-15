// Global transaction table implementation for cross-engine transactions.

#include "handler.h"
#include "gtt.h"

extern handlerton *ermia_hton;

pthread_rwlock_t GlobalTransactionTable::lock;

// Caller manage the varstr and the buf
static inline void encode_lsn(uint64_t lsn, uint8_t *buf, ermia::varstr &out) {
    uint64_t tmp = htobe64(lsn);
    memcpy(buf, &tmp, sizeof(tmp));
    out.l = sizeof(tmp);
    out.p = buf;
}

// TODO: make this a knob
static const uint32_t kMaxSize = 20;

GlobalTransactionTable::GlobalTransactionTable(uint32_t cap) : epoch(ermia::MM::mm_epochs.cb) {
  int ret = pthread_rwlock_init(&lock, nullptr);
  ALWAYS_ASSERT(ret == 0);

  // Create an empty tree so there always at least one index available
  // This simplifies the logic for processing the very first transaction
  auto *ci = new Index(cap, 0);
  commit_indexes.push_back(ci);
}

GlobalTransactionTable::~GlobalTransactionTable() { 
  int ret = pthread_rwlock_destroy(&lock);
  ALWAYS_ASSERT(ret == 0);
  LOG(INFO) << "Used" << commit_indexes.size() << " indexes" << std::endl;
}
// The inserted transaction should have a inno_trx_id no smaller than
// the prev entry, no greater than the next entry

// E.g: If we want to insert B, lsnX
// We need to ensure lsnX lies in [A.max_inno_trx_id, C.min_inno_trx_id] inclusive
// Table:
// elsn | inno_trx_id_list
// A    | lsn0, lsn1, lsn2, lsn3 ...
// B    |
// C    | lsn100, lsn101, lsn...
//
// @pre_ermia_lsn: ERMIA commit LSN
// @inno_trx_id: InnoDB transaction ID
bool GlobalTransactionTable::commit_check(uint64_t pre_ermia_lsn, uint64_t inno_trx_id) {
  ermia::varstr keystr;
  uint8_t buf[8];
  bool lock_gtt_shared = true;
  epoch_init_once();
  auto ep = epoch.thread_enter();

retry:

  GlobalTransactionTable::Index *ci = nullptr;
  GTTLockGuard guard(lock_gtt_shared);
  ci = get_index(pre_ermia_lsn);
  ALWAYS_ASSERT(ci);

  // Lock the entire index
  std::lock_guard<std::mutex> lg(ci->lock);

  // Get list A
  encode_lsn(pre_ermia_lsn - 1, buf, keystr); // first elsn < pre_ermia_lsn entry
  GTTFetchCallback cb;
  ci->mtree.rsearch_range_oid(keystr, nullptr, cb, ep);
  trx_id_list *list_a = nullptr;
  if (cb.found()) {
    list_a = reinterpret_cast<trx_id_list *>(cb.result);
  }

  // Get list C
  cb.result = 0;
  encode_lsn(pre_ermia_lsn + 1, buf, keystr);
  trx_id_list *list_c = nullptr;
  ci->mtree.search_range_oid(keystr, nullptr, cb, ep);
  if (cb.found()) {
    list_c = reinterpret_cast<trx_id_list *>(cb.result);
  }

  // Get list B in case we need to insert later
  encode_lsn(pre_ermia_lsn, buf, keystr);
  trx_id_list *list_b = nullptr;
  bool list_b_exists = ci->mtree.search(keystr, (uintptr_t&)list_b, ep, nullptr);

  // Now check commit compatibility
  if (list_a) {
    auto &entries = list_a->entries;
    ALWAYS_ASSERT(entries.size());
    // A.max_lsn_id shouldn't be greater than our B.inno_trx_id
    if (entries[entries.size() - 1] > inno_trx_id) {
      epoch.thread_exit();
      return false;
    }
  }

  // Continue to check list C
  if (list_c) {
    auto &entries = list_c->entries;
    ALWAYS_ASSERT(entries.size());

    // C.max_lsn_id shouldn't be smaller than our B.inno_trx_id
    if (entries[0] < inno_trx_id) {
      epoch.thread_exit();
      return false;
    }
  }

  // All good, insert to list b
  if (list_b_exists) {
    // Here is a tradeoff - we limit the max size of the entries vector to be a
    // constant to avoid too long list which is time consuming to sort. Note
    // that the num_entries variable refers to the number of keys, instead of
    // real number of mappings which is SUM(# entries per list for each key).
    if (list_b->entries.size() >= kMaxSize) {
      list_b->entries[0] = inno_trx_id;
    } else {
      list_b->entries.push_back(inno_trx_id);
    }
    std::sort(list_b->entries.begin(), list_b->entries.end());
    DLOG(INFO) << "Inserted <" << pre_ermia_lsn << ", " << inno_trx_id << ">";
  } else {
    // Do the insert, but be careful this tree might be full
    list_b = new trx_id_list;
    list_b->entries.push_back(inno_trx_id);
    if (!ci->allow_insert()) {
      if (ci == commit_indexes[commit_indexes.size() - 1]) {
        /* This is the latest tree, we can insert into a new tree */
          if (lock_gtt_shared) {
            delete list_b;
            lock_gtt_shared = false;
            goto retry;
          } else {
            ci = new Index(ermia_hton->gtt->get_capacity(), pre_ermia_lsn);
            commit_indexes.push_back(ci);
          }
      } else {
        /* No luck this time */
        epoch.thread_exit();
        return false;
      }
    }
    bool success = ci->mtree.insert(keystr, reinterpret_cast<uintptr_t>(list_b), ep);
    if (success) {
      ++ci->num_entries;
      DLOG(INFO) << "Inserted <" << pre_ermia_lsn << ", " << inno_trx_id << "> (new list)";
    } else {
      epoch.thread_exit();
      goto retry;
    }
  }

  epoch.thread_exit();
  return true;
}

/*
 * When opening an innodb read view with ermia already touched, it will go through this method to
 * get a correct InnoDB trx_id to consturct a read view.
 *
 * @lsn: ERMIA LSN
 * @fn: function to call to obtain a new InnoDB read view when needed.
 */
uint64_t GlobalTransactionTable::get_innodb_trx_id(uint64_t lsn, std::function<uint64_t(void)> fn) {
  epoch_init_once();
  auto ep = epoch.thread_enter();
  bool lock_gtt_shared = true;

retry:
  // Prepare key for masstree
  ermia::varstr keystr;
  uint8_t buf[8];
  encode_lsn(lsn, buf, keystr);
  GlobalTransactionTable::Index *ci = nullptr;

  // Take shared lock for the gtt lock for a stable commit_indexes list
  GTTLockGuard gtt_guard(lock_gtt_shared);

  // Obtain and lock the index
  ci = get_index(lsn);

  if (!ci) {
    // No index available - the transaction is too old, must abort
    return 0;
  }

  // Lock the index
  ci->lock.lock();

  // Try exact match first
  trx_id_list *list = nullptr;
  bool found = ci->mtree.search(keystr, (uint64_t&)list, ep, nullptr);
  uint64_t trx_id = 0;

  // See if we can obtain a new, latest InnoDB readview (first we need to be
  // using the latest index
  bool use_latest = false;
  if (ci == commit_indexes[commit_indexes.size() - 1]) {
    encode_lsn(lsn + 1, buf, keystr);
    GTTFetchCallback cb;
    ci->mtree.search_range_oid(keystr, nullptr, cb, ep);
    if (!cb.found()) {
      use_latest = true;
    }
  }
  encode_lsn(lsn, buf, keystr);

  if (found) {
    // We found the ERMIA lsn, just use the max trx_id under it or the latest
    // trx_id if allowed, and then we are done.
    ALWAYS_ASSERT(list);
    ALWAYS_ASSERT(list->entries.size());

    // Any trx_id in the list in fact would work. But we pick the latest, this
    // would cover the corner case where the ERMIA side doesn't move but only
    // InnoDB changes. Also, if we guarantee there is no skewed snapshot will be
    // formed, we can take the latest innodb trx id here.

    if (use_latest) {
      trx_id = fn();
      if (list->entries.size() >= kMaxSize) {
        list->entries[0] = trx_id;
      } else {
        list->entries.push_back(trx_id);
      }
      std::sort(list->entries.begin(), list->entries.end());
    } else {
      trx_id = list->entries[list->entries.size() - 1];
    }
  } else {
    // No exact match - it could be because no global transaction has ever run yet, so the commit
    // index is empty, or it's been a while that there hasn't been a global transaction (i.e., only
    // ERMIA side is moving). In either case, we just need to get the latest innodb trx id that was
    // recorded but correspons to an LSN smaller than [lsn], i.e., need to find the nearest
    // neighbour whose ERMIA LSN (elsn) is < my ERMIA LSN (lsn parameter passed in). 
    if (!ci->allow_insert()) {
      // Index full, insert not allowed - check if [ci] is actually the latest,
      // if so create a new tree; otherwise have to give up.
      // Since we need to touch the index array, must take GTT lock in EX mode
      ci->lock.unlock();

      if (use_latest && ci == commit_indexes[commit_indexes.size() - 1]) {
        if (lock_gtt_shared) {
          lock_gtt_shared = false;
          goto retry;
        } else {
          ci = new Index(ermia_hton->gtt->get_capacity(), lsn);
          commit_indexes.push_back(ci);
          ci->lock.lock();
        }
        DLOG(INFO) <<  "========== [get_innodb_trx_id] Created new index " << commit_indexes.size();
      } else {
        DLOG(WARNING) << "Unable to obtained trx_id for " << lsn;
        epoch.thread_exit();
        return 0;
      }
    }

    if (ci->is_empty() or use_latest) {
      // No read view, safe to use the latest TID (note that trx_sys->max_trx_id is the TID
      // that's not yet assigned)
      trx_id = fn();
    } else {
      GTTFetchCallback cb;
      encode_lsn(lsn, buf, keystr);
      ci->mtree.rsearch_range_oid(keystr, nullptr, cb, ep);
      if (!cb.found()) {
        // No luck with this tree - for now bail out. An alternative is to check further other trees
        // but that would risk using a very old read view.
        ci->lock.unlock();
        epoch.thread_exit();
        return 0;
      }

      // Some readview exists, use it
      // XXX: it seems this can be combined with the search case before
      trx_id_list *prev_list = (trx_id_list *)cb.result;
      if (prev_list->entries.size() == 0) {
        // Someone else just inserted a new index, retry
        ci->lock.unlock();
        goto retry;
      } else {
        trx_id = prev_list->entries[prev_list->entries.size() - 1];
      }
    }
    // Setup a new lsn->trx_id mapping
    list = new trx_id_list;
    list->entries.push_back(trx_id);
    bool inserted = ci->mtree.insert(keystr, reinterpret_cast<uintptr_t>(list), ep);
    DLOG_IF(FATAL, !inserted) << "Insert should always succeed";
    if (!inserted) {
      // Someone else acted faster, retry
      ci->lock.unlock();
      delete list;
      goto retry;
    }
    ++ci->num_entries;
  }

  ci->lock.unlock();
  epoch.thread_exit();
  DLOG(INFO) << "Obtained trx_id " << trx_id << " for " << lsn;
  return trx_id;
}

/**
   Deallocate trees that no transaction will ever use.
*/
void GlobalTransactionTable::recycle(uint64_t min_active_elsn) {
  if (commit_indexes.size() <= ermia_hton->gtt->get_threshold()) {
      return ;
  }
  DLOG(INFO) << "Size larger than threshold, current size = " 
             << commit_indexes.size() << " ,begin recycle.";

  GTTLockGuard guard(false);
  uint32_t total = commit_indexes.size();

  // Identify the latest tree whose elsn is <= min_active_elsn.
  // Then trees older than it can all be deallocated.
  int32_t nfree = (int32_t)commit_indexes.size();
  while (--nfree > 0) {
    if (commit_indexes[nfree]->min_elsn < min_active_elsn) {
      break;
    }
  }

  int32_t recycled = nfree;
  gtt_tree_count -= recycled;

  // Elements from index 0 to nfree-1 (inclusive) can be freed
  while (--nfree> 0) {
    DLOG(INFO) << "Recycled tree with elsn=0x" << std::hex << commit_indexes[0]->min_elsn;
    delete commit_indexes[0];
    commit_indexes.erase(commit_indexes.begin());
  }

  DLOG(INFO) << "Recycled " << recycled << " out of " << total
             << " trees (min_active_elsn=" << min_active_elsn << ")";
}

// Caller must hold gtt.lock
GlobalTransactionTable::Index *GlobalTransactionTable::get_index(uint64_t elsn) {
  GlobalTransactionTable::Index *ci = nullptr;
  if (commit_indexes.size()) {
    // Fast forward to the first tree whose elsn is <= my LSN
    int32_t ci_idx = 0;
    for (ci_idx = commit_indexes.size() - 1; ci_idx >= 0; ci_idx--) {
      ci = commit_indexes[ci_idx];
      if (ci->min_elsn <= elsn) {
        break;
      }
    }
  }
  return ci;
}
