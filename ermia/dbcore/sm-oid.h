#pragma once

#include "epoch.h"
#include "sm-common.h"
#include "sm-oid-alloc-impl.h"
#include "sm-log.h"

#include "dynarray.h"

#include "../macros.h"
#include "../tuple.h"

namespace ermia {

#define LOCK_MASK (1 << 31)

inline void XLock(OID *lock) {
    int32_t *ptr = reinterpret_cast<int32_t *>(lock);
    int32_t expected = 0;
    int32_t locked = expected | LOCK_MASK;
    while (volatile_read(*ptr) != expected or
            !__atomic_compare_exchange_n(ptr, &expected, locked, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        expected = 0;
    }
}

inline void XUnlock(OID *lock) {
    int32_t *ptr = reinterpret_cast<int32_t *>(lock);
    __atomic_store_n(ptr, 0, __ATOMIC_RELEASE);
}

inline void RLock(OID *lock) {
    int32_t *ptr = reinterpret_cast<int32_t *>(lock);
retry_x:
    while(volatile_read(*ptr) & LOCK_MASK);
retry_s:
    int32_t lock_word = __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
    if (lock_word & LOCK_MASK) goto retry_x;
    int32_t desired = lock_word + 1;
    bool ok = __atomic_compare_exchange_n(ptr, &lock_word, desired, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    if (!ok) goto retry_s;
}

inline void RUnlock(OID *lock) {
    int32_t *ptr = reinterpret_cast<int32_t *>(lock);
    __atomic_fetch_sub(ptr, 1, __ATOMIC_ACQ_REL);
}


typedef epoch_mgr::epoch_num epoch_num;

/* OID arrays and allocators alike always occupy an integer number
   of dynarray pages, to ensure that we don't hit precision issues
   when saving dynarray contents to (and restoring from)
   disk. It's also more than big enough for the objects to reach
   full size
 */
static size_t const SZCODE_ALIGN_BITS = dynarray::page_bits();

/* An OID is essentially an array of fat_ptr. The only bit of
   magic is that it embeds the dynarray that manages the storage
   it occupies.
 */
struct oid_array {
  static size_t const MAX_SIZE = sizeof(fat_ptr) << 32;
  static uint64_t const MAX_ENTRIES =
      (size_t(1) << 32) - sizeof(dynarray) / sizeof(fat_ptr);
  static size_t const ENTRIES_PER_PAGE =
      (sizeof(fat_ptr) << SZCODE_ALIGN_BITS) / 2;

  /* How much space is required for an array with [n] entries?
   */
  static size_t alloc_size(size_t n = MAX_ENTRIES) {
    return OFFSETOF(oid_array, _entries[n]);
  }

  static fat_ptr make();

  static dynarray make_oid_dynarray() {
    return dynarray(oid_array::alloc_size(), 128 * config::MB);
  }

  static void destroy(oid_array *oa);

  oid_array(dynarray &&owner);

  // unsafe!
  oid_array() = delete;
  oid_array(oid_array const &) = delete;
  void operator=(oid_array) = delete;

  /* Return the number of entries this OID array currently holds.
   */
  inline size_t nentries() { return _backing_store.size() / sizeof(fat_ptr); }

  /* Make sure the backing store holds at least [n] entries.
   */
  void ensure_size(size_t n);

  /* Return a pointer to the given OID's slot.

     WARNING: The caller is responsible for handling races in
     case multiple threads try to update the slot concurrently.

     WARNING: this function does not perform bounds
     checking. The caller is responsible to use nentries() and
     ensure_size() as needed.
   */
  fat_ptr *get(OID o) { return &_entries[o]; }

  dynarray _backing_store;
  fat_ptr _entries[];
};

struct sm_oid_mgr {
  using log_tx_scan = sm_log_scan_mgr::record_scan;

  /* Metadata for any allocated file can be stored in this file at
     the OID that matches its FID.
   */
  static FID const METADATA_FID = 2;

  /* Create a new OID manager.

     NOTE: the scan must be positioned at the first record of the
     checkpoint transaction (or the location where the record would
     be if the checkpoint is empty). When this function returns, the
     scan will be positioned at whatever follows the OID checkpoint
     (or invalid, if there are no more records).

     tzwang: above is the orignal interface design. The implementation
     here is to checkpoint the OID arrays to an individual file, whose
     name is in the format of "chd-[chkpt-start-LSN]"; after successfully
     written this file, we use the [chkpt start LSN, chkpt end LSN] pair
     as an empty file's filename to denote this chkpt was successful.
     sm_oid_mgr::create() then accepts the chkpt start LSN to know which
     chkpt file to look for, and recovers from the chkpt file, followed by
     a log scan (if needed). The chkpt files are stored in the log dir.

     The separation of the chkpt from the log reduces interference to
     normal transaction processing during checkpointing; storing the
     chkpt file in a separate file, instead of in the log, reduces the
     amount of data to ship for replication (no need to ship chkpts,
     the backup can have its own chkpts).
   */
  static void create();

  /* Record a snapshot of the OID manager's state as part of a
     checkpoint. The data will be durable by the time this function
     returns, but will only be reachable if the checkpoint
     transaction commits and its location is properly recorded.
   */
  void PrimaryTakeChkpt();

  /* Create a new file and return its FID. If [needs_alloc]=true,
     the new file will be managed by an allocator and its FID can be
     passed to alloc_oid(); otherwise, the file is either unmanaged
     or a slave to some other file.
   */
  FID create_file(bool needs_alloc = true);

  /* Destroy file [f] and remove its contents. Its allocator, if
     any, will also be removed.

     The caller is responsible to destroy any "slave" files that
     depended on this one, and to remove the file's metadata entry
     (if any).

     WARNING: the caller is responsible to ensure that this file is
     no longer in use by other threads.
   */
  void destroy_file(FID f);

  /* Allocate a new OID in file [f] and return it.

     Throw fid_is_full if no more OIDs are available for allocation
     in this file.

     WARNING: This is a volatile operation. In the event of a crash,
     the OID will be freed unless recorded in some way (usually an
     insert log record).

  */
  OID alloc_oid(FID f);

  /* Free an OID and return its contents (which should be disposed
     of by the caller as appropriate).

     WARNING: This is a volatile operation. In the event of a crash,
     the OID will be freed unless recorded in some way (usually a
     delete log record).
  */
  fat_ptr free_oid(FID f, OID o);

  /* Retrieve the raw contents of the specified OID. The returned
     fat_ptr may reference memory or disk.
   */
  fat_ptr oid_get(FID f, OID o);
  fat_ptr *oid_get_ptr(FID f, OID o);

  /* Update the contents of the specified OID. The fat_ptr may
     reference memory or disk (or be NULL).
   */
  void oid_put(FID f, OID o, fat_ptr p);
  void oid_put_new(FID f, OID o, fat_ptr p);
  void oid_put_new_if_absent(FID f, OID o, fat_ptr p);

  /* Return a fat_ptr to the overwritten object (could be an in-flight version!)
   */
  fat_ptr PrimaryTupleUpdate(FID f, OID o, const varstr *value,
                             TXN::xid_context *updater_xc, fat_ptr *new_obj_ptr);
  fat_ptr PrimaryTupleUpdate(oid_array *oa, OID o, const varstr *value,
                             TXN::xid_context *updater_xc, fat_ptr *new_obj_ptr);

  dbtuple *oid_get_latest_version(FID f, OID o);

  dbtuple *oid_get_version(FID f, OID o, TXN::xid_context *visitor_xc);
  dbtuple *oid_get_version(oid_array *oa, OID o, TXN::xid_context *visitor_xc);
  dbtuple *oid_get_s2pl(oid_array *oa, OID o, TXN::xid_context *visitor_xc, bool for_write, rc_t &out_rc);

  void oid_get_version_backup(fat_ptr &ptr,
                              fat_ptr &tentative_next,
                              Object *prev_obj,
                              Object *&cur_obj,
                              TXN::xid_context *visitor_xc);

  /* Return the latest visible version, for backups only. Check first the pedest
   * array and install new Objects on the tuple array if needed.
   */
  dbtuple *BackupGetVersion(oid_array *ta, oid_array *pa, OID o,
                            TXN::xid_context *xc);

  /* Helper function for oid_get_version to test visibility. Returns true if the
   * version ([object]) is visible to the given transaction ([xc]). Sets [retry]
   * to true if the caller needs to retry the search from the head of the chain.
   */
  bool TestVisibility(Object *object, TXN::xid_context *xc, bool &retry);

  inline void oid_check_phantom(TXN::xid_context *visitor_xc, uint64_t vcstamp) {
#if !defined(SSI) && !defined(SSN)
    MARK_REFERENCED(visitor_xc);
    MARK_REFERENCED(vcstamp);
#endif
    if (!config::phantom_prot) {
      return;
    }
/*
 * tzwang (May 05, 2016): Preventing phantom:
 * Consider an example:
 *
 * Assume the database has tuples B (key=1) and C (key=2).
 *
 * Time      T1             T2
 * 1        ...           Read B
 * 2        ...           Insert A
 * 3        ...           Commit
 * 4       Scan key > 1
 * 5       Update B
 * 6       Commit (?)
 *
 * At time 6 T1 should abort, but checking index version changes
 * wouldn't make T1 abort, since its scan happened after T2's
 * commit and yet its begin timestamp is before T2 - T1 wouldn't
 * see A (oid_get_version will skip it even it saw it from the tree)
 * but the scanning wouldn't record a version change in tree structure
 * either (T2 already finished all SMOs).
 *
 * Under SSN/SSI, this essentially requires we update the corresponding
 * stamps upon hitting an invisible version, treating it like some
 * successor updated our read set. For SSI, this translates to updating
 * ct3; for SSN, update the visitor's sstamp.
 */
#ifdef SSI
    auto vct3 = volatile_read(visitor_xc->ct3);
    if (not vct3 or vct3 > vcstamp) {
      volatile_write(visitor_xc->ct3, vcstamp);
    }
#elif defined SSN
    visitor_xc->set_sstamp(std::min(visitor_xc->sstamp.load(), vcstamp));
// TODO(tzwang): do early SSN check here
#endif  // SSI/SSN
  }

  inline Object *oid_get_latest_object(oid_array *oa, OID o) {
    auto head_offset = oa->get(o)->offset();
    if (head_offset) {
      return (Object *)head_offset;
    }
    return NULL;
  }

  inline dbtuple *oid_get_latest_version(oid_array *oa, OID o) {
    auto head_offset = oa->get(o)->offset();
    if (head_offset) return (dbtuple *)((Object *)head_offset)->GetPayload();
    return NULL;
  }

  inline void PrimaryTupleUnlink(oid_array *oa, OID o) {
    // Now the head is guaranteed to be the only dirty version
    // because we unlink the overwritten dirty version in put,
    // essentially this function ditches the head directly.
    // Otherwise use the commented out old code.
    PrimaryTupleUnlink(oa->get(o));
  }
  inline void PrimaryTupleUnlink(fat_ptr *ptr) {
    Object *head_obj = (Object *)ptr->offset();
    // using a CAS is overkill: head is guaranteed to be the (only) dirty
    // version
    volatile_write(ptr->_ptr, head_obj->GetNextVolatile()._ptr);
    __sync_synchronize();
    // tzwang: The caller is responsible for deallocate() the head version
    // got unlinked - a update of own write will record the unlinked version
    // in the transaction's write-set, during abortor commit the version's
    // pvalue needs to be examined. So PrimaryTupleUnlink() shouldn't
    // deallocate()
    // here. Instead, the transaction does it in during commit or abort.
  }

  inline void oid_put_new(oid_array *oa, OID o, fat_ptr p) {
    auto *entry = oa->get(o);
    ASSERT(*entry == NULL_PTR);
    *entry = p;
  }

  inline void oid_put(oid_array *oa, OID o, fat_ptr p) {
    auto *ptr = oa->get(o);
    *ptr = p;
  }

  inline fat_ptr oid_get(oid_array *oa, OID o) { return *oa->get(o); }
  inline fat_ptr *oid_get_ptr(oid_array *oa, OID o) { return oa->get(o); }

  inline OID *dirp(oid_array *oa, OID o) {
    auto p = oa->get(o);
    ALWAYS_ASSERT(p->asi() & fat_ptr::ASI_DIR);
    auto pp = reinterpret_cast<OID *>(p->offset());
    return pp;
  }
  bool oid_get_dir(oid_array *oa, OID o, std::vector<OID> &out_vec) {
    auto root_oid_ptr = dirp(oa, o);
    RLock(root_oid_ptr + OID_DIR_LATCH_INDEX);
    DEFER(RUnlock(root_oid_ptr + OID_DIR_LATCH_INDEX));
    auto rec_count = reinterpret_cast<uint32_t>(root_oid_ptr[0]);
    /*
     * Only the 0-level oid_dir record stores from index 2, 0 to 1(inclusive)
     * is used for store header
     */
    auto idx = OID_DIR_HEADER_SIZE;
    auto cur_dir = root_oid_ptr;
    while (rec_count) {
        if (idx != OID_DIR_SIZE - 1) {
            DLOG(INFO) << "Pushing OID = 0x" << std::hex << cur_dir[idx] << " into outvec";
            out_vec.push_back(cur_dir[idx++]);
            rec_count -= 1;
            continue;
        }
        cur_dir = reinterpret_cast<OID *>(oa->get(cur_dir[idx])->offset());
        DLOG(INFO) << "Following the next level sub-dir: " << cur_dir;
        idx = 0;
    }
    return true;
  }

  inline OID dir_get_index(oid_array *oa, OID *rootp, uint64_t index, bool &eof) {
    /* Index starts from 1 */
    ALWAYS_ASSERT(index > 0);
    RLock(rootp + OID_DIR_LATCH_INDEX);
    DEFER(RUnlock(rootp + OID_DIR_LATCH_INDEX));
    auto rec_count = reinterpret_cast<uint32_t>(rootp[0]);
    if (index <= rec_count) {
        eof = false;
    } else {
        eof = true;
        return INVALID_OID;
    }
    auto layer = (index + OID_DIR_HEADER_SIZE - 1) / (OID_DIR_SIZE - 1);
    auto pos = (index + OID_DIR_HEADER_SIZE - 1) % (OID_DIR_SIZE - 1);
    ALWAYS_ASSERT(pos != OID_DIR_HEADER_SIZE - 1);
    ALWAYS_ASSERT(layer >= 0);
    auto cur_dir = rootp;
    while(layer != 0) {
        cur_dir = reinterpret_cast<OID *>(oa->get(cur_dir[OID_DIR_SIZE - 1])->offset());
        layer -= 1;
    }
    return cur_dir[pos];
  }


  /*
   * Return the next layer and pos as well as the current OID
   */
  OID dir_get_next_pos(oid_array *oa, OID dir_oid, uint64_t &layer, uint64_t &pos) {
    ALWAYS_ASSERT(pos != OID_DIR_HEADER_SIZE - 1);
    ALWAYS_ASSERT(layer >= 0);
    auto rootp = dirp(oa, dir_oid);

    RLock(rootp + OID_DIR_LATCH_INDEX);
    DEFER(RUnlock(rootp + OID_DIR_LATCH_INDEX));

    int64_t index = 0;
    if (layer > 0) {
        index = (OID_DIR_SIZE - OID_DIR_HEADER_SIZE - 1) + (layer - 1) * (OID_DIR_SIZE - 1) + pos + 1;
    } else {
        index = pos - OID_DIR_HEADER_SIZE + 1;
    }
    // TODO(jianqiuz): Do we need this function?
    return INVALID_OID;
  }

  bool file_exists(FID f);
  void recreate_file(FID f);              // for recovery only
  void recreate_allocator(FID f, OID m);  // for recovery only
  oid_array *get_array(FID f);
  sm_allocator *get_allocator(FID f);

  static void warm_up();
  void start_warm_up();

  int dfd;  // dir for storing OID chkpt data file

  virtual ~sm_oid_mgr() {}

 protected:
  // Forbid direct instantiation
  sm_oid_mgr() {}
};

extern sm_oid_mgr *oidmgr;
}  // namespace ermia
