#include "sm-alloc.h"
#include "sm-chkpt.h"
#include "sm-log.h"
#include "sm-log-recover.h"
#include "sm-object.h"
#include "../tuple.h"

namespace ermia {

// Dig out the payload from the durable log
// ptr should point to some position in the log and its size_code should refer
// to only data size (i.e., the size of the payload of dbtuple rounded up).
// Returns a fat_ptr to the object created
void Object::Pin(bool load_from_logbuf) {
  uint32_t status = volatile_read(status_);
  if (status != kStatusStorage) {
    if (status == kStatusLoading) {
      while (volatile_read(status_) != kStatusMemory) {
      }
    }
    ALWAYS_ASSERT(volatile_read(status_) == kStatusMemory ||
                  volatile_read(status_) == kStatusDeleted);
    return;
  }

  // Try to 'lock' the status
  // TODO: have the thread do something else while waiting?
  uint32_t val =
      __sync_val_compare_and_swap(&status_, kStatusStorage, kStatusLoading);
  if (val == kStatusMemory) {
    return;
  } else if (val == kStatusLoading) {
    while (volatile_read(status_) != kStatusMemory) {
    }
    return;
  } else {
    ASSERT(val == kStatusStorage);
    ASSERT(volatile_read(status_) == kStatusLoading);
  }

  uint32_t final_status = kStatusMemory;

  // Now we can load it from the durable log
  ALWAYS_ASSERT(pdest_.offset());
  uint16_t where = pdest_.asi_type();
  ALWAYS_ASSERT(where == fat_ptr::ASI_LOG || where == fat_ptr::ASI_CHK);

  // Already pre-allocated space when creating the object
  dbtuple *tuple = (dbtuple *)GetPayload();
  new (tuple) dbtuple(0);  // set the correct size later

  size_t data_sz = decode_size_aligned(pdest_.size_code());
  if (where == fat_ptr::ASI_LOG) {
    ASSERT(logmgr);
    // Not safe to dig out from the log buffer as it might be receiving a
    // new batch from the primary, unless we have NVRAM as log buffer.
    // XXX: for now we can't flush - need coordinate with backup daemon
    if (config::is_backup_srv() && !config::nvram_log_buffer) {
      while (pdest_.offset() >= logmgr->durable_flushed_lsn().offset()) {
      }
    }

    // Load tuple varstr from the log
    if (load_from_logbuf) {
      logmgr->load_object_from_logbuf((char *)tuple->get_value_start(), data_sz,
                                      pdest_);
    } else {
      logmgr->load_object((char *)tuple->get_value_start(), data_sz, pdest_);
    }
    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    // Fill in the overwritten version's pdest if needed
    if (config::is_backup_srv() && next_pdest_ == NULL_PTR) {
      next_pdest_ = ((varstr *)tuple->get_value_start())->ptr;
    }
    // Could be a delete
    ASSERT(tuple->size < data_sz);
    if (tuple->size == 0) {
      final_status = kStatusDeleted;
      ASSERT(next_pdest_.offset());
    }
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr), tuple->size);
    SetClsn(LSN::make(pdest_.offset(), 0).to_log_ptr());
    ALWAYS_ASSERT(pdest_.offset() == clsn_.offset());
  } else {
    // Load tuple data form the chkpt file
    ASSERT(sm_chkpt_mgr::base_chkpt_fd);
    ALWAYS_ASSERT(pdest_.offset());
    ASSERT(volatile_read(status_) == kStatusLoading);
    // Skip the status_ and alloc_epoch_ fields
    static const uint32_t skip = sizeof(status_) + sizeof(alloc_epoch_);
    uint32_t read_size = data_sz - skip;
    auto n = os_pread(sm_chkpt_mgr::base_chkpt_fd, (char *)this + skip,
                      read_size, pdest_.offset() + skip);
    ALWAYS_ASSERT(n == read_size);
    ASSERT(tuple->size <= read_size - sizeof(dbtuple));
    next_pdest_ = NULL_PTR;
  }
  ASSERT(clsn_.asi_type() == fat_ptr::ASI_LOG);
  ALWAYS_ASSERT(pdest_.offset());
  ALWAYS_ASSERT(clsn_.offset());
  ASSERT(volatile_read(status_) == kStatusLoading);
  SetStatus(final_status);
}

fat_ptr Object::Create(const varstr *tuple_value, bool do_write,
                       epoch_num epoch) {
  if (tuple_value) {
    do_write = true;
  }

  // Calculate tuple size
  const uint32_t data_sz = tuple_value ? tuple_value->size() : 0;
  size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;

  // Allocate a version
  Object *obj = new (MM::allocate(alloc_sz)) Object();
  // In case we got it from the tls reuse pool
  ASSERT(obj->GetAllocateEpoch() <= epoch - 4);
  obj->SetAllocateEpoch(epoch);

  // Tuple setup
  dbtuple *tuple = (dbtuple *)obj->GetPayload();
  new (tuple) dbtuple(data_sz);
  ASSERT(tuple->pvalue == NULL);
  tuple->pvalue = (varstr *)tuple_value;
  if (do_write) {
    tuple->DoWrite();
  }

  size_t size_code = encode_size_aligned(alloc_sz);
  ASSERT(size_code != INVALID_SIZE_CODE);
  return fat_ptr::make(obj, size_code, 0 /* 0: in-memory */);
}

// Make sure the object has a valid clsn/pdest
fat_ptr Object::GenerateClsnPtr(uint64_t clsn) {
  fat_ptr clsn_ptr = NULL_PTR;
  uint64_t tuple_off = GetPersistentAddress().offset();
  if (tuple_off == 0) {
    // Must be a delete record
    ASSERT(GetPinnedTuple()->size == 0);
    ASSERT(GetPersistentAddress() == NULL_PTR);
    tuple_off = clsn;
    clsn_ptr = LSN::make(tuple_off, 0).to_log_ptr();
    // Set pdest here which wasn't set by log_delete
    pdest_ = clsn_ptr;
  } else {
    clsn_ptr = LSN::make(tuple_off, 0).to_log_ptr();
  }
  return clsn_ptr;
}
}  // namespace ermia
