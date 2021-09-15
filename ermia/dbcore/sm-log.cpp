#include "sm-log-impl.h"
#include "sm-log-offset.h"
#include "sm-oid.h"
#include "sm-oid-impl.h"
#include "sm-thread.h"
#include <cstring>

namespace ermia {

sm_log *logmgr = NULL;
bool sm_log::need_recovery = false;
window_buffer *sm_log::logbuf = nullptr;

void sm_log::create_segment_file(segment_id *sid) {
  get_impl(this)->_lm._lm.create_segment_file(sid);
}

void sm_log::allocate_log_buffer() {
  logbuf = new window_buffer(config::log_buffer_mb * config::MB);
}

segment_id *sm_log::get_offset_segment(uint64_t off) {
  return get_impl(this)->_lm._lm.get_offset_segment(off);
}

segment_id *sm_log::get_segment(uint32_t segnum) {
  return get_impl(this)->_lm._lm.get_segment(segnum);
}

LSN sm_log::get_chkpt_start() {
  return get_impl(this)->_lm._lm.get_chkpt_start();
}

void sm_log::set_tls_lsn_offset(uint64_t offset) {
  get_impl(this)->_lm.set_tls_lsn_offset(offset);
}

uint64_t sm_log::get_tls_lsn_offset() {
  return get_impl(this)->_lm.get_tls_lsn_offset();
}

sm_log_recover_impl *sm_log::get_backup_replay_functor() {
  return get_impl(this)->_lm._lm.get_backup_replay_functor();
}

window_buffer *sm_log::get_logbuf() { return sm_log::logbuf; }

void sm_log::redo_log(LSN start_lsn, LSN end_lsn) {
  get_impl(this)->_lm._lm.redo_log(start_lsn, end_lsn);
}

LSN sm_log::backup_redo_log_by_oid(LSN start_lsn, LSN end_lsn) {
  return get_impl(this)->_lm._lm.backup_redo_log_by_oid(start_lsn, end_lsn);
}

void sm_log::start_logbuf_redoers() {
  get_impl(this)->_lm._lm.start_logbuf_redoers();
}

void sm_log::recover() { get_impl(this)->_lm._lm.recover(); }

segment_id *sm_log::assign_segment(uint64_t lsn_begin, uint64_t lsn_end) {
  auto rval = get_impl(this)->_lm._lm.assign_segment(lsn_begin, lsn_end);
  ALWAYS_ASSERT(rval.full_size);
  return rval.sid;
}

void sm_log::BackupFlushLog(uint64_t new_dlsn_offset) {
  return get_impl(this)->_lm.BackupFlushLog(new_dlsn_offset);
}

void sm_log::enqueue_committed_xct(uint32_t worker_id, rLSN &rlsn, uint64_t start_time,
                                   std::function<void(void *)> callback, void *context) {
  get_impl(this)->_lm.enqueue_committed_xct(worker_id, rlsn, start_time, callback, context);
}

void sm_log::free_tls_lsn_slot() {
  get_impl(this)->_lm.free_tls_lsn_slot();
}

void sm_log::set_upto_lsn(LSNType type, uint64_t lsn) {
  return get_impl(this)->_lm.set_upto_lsn(type, lsn);
}

LSN sm_log::flush() { return get_impl(this)->_lm.flush(); }

void sm_log::update_chkpt_mark(LSN cstart, LSN cend) {
  get_impl(this)->_lm._lm.update_chkpt_mark(cstart, cend);
}

void sm_log::load_object(char *buf, size_t bufsz, fat_ptr ptr,
                         size_t align_bits) {
  get_impl(this)->_lm._lm.load_object(buf, bufsz, ptr, align_bits);
}

void sm_log::load_object_from_logbuf(char *buf, size_t bufsz, fat_ptr ptr,
                                     size_t align_bits) {
  get_impl(this)->_lm._lm.load_object_from_logbuf(buf, bufsz, ptr, align_bits);
}

fat_ptr sm_log::load_ext_pointer(fat_ptr ptr) {
  return get_impl(this)->_lm._lm.load_ext_pointer(ptr);
}

int sm_log::open_segment_for_read(segment_id *sid) {
  return get_impl(this)->_lm._lm.open_for_read(sid);
}

sm_log *sm_log::new_log(sm_log_recover_impl *recover_functor, void *rarg) {
  need_recovery = false;
  if (config::null_log_device) {
    dirent_iterator iter(config::log_dir.c_str());
    for (char const *fname : iter) {
      if (strcmp(fname, ".") and strcmp(fname, ".."))
        os_unlinkat(iter.dup(), fname);
    }
  }
  ALWAYS_ASSERT(config::log_segment_mb);
  ALWAYS_ASSERT(config::log_buffer_mb);
  return new sm_log_impl(recover_functor, rarg);
}

sm_log_scan_mgr *sm_log::get_scan_mgr() {
  return get_impl(this)->_lm._lm.scanner;
}

sm_tx_log *sm_log::new_tx_log(char *log_space) {
  auto *self = get_impl(this);
  typedef _impl_of<sm_tx_log>::type Impl;
  return new (log_space) Impl(self);
}

fat_ptr sm_log_impl::lsn2ptr(LSN lsn, bool is_ext) {
  return get_impl(this)->_lm._lm.lsn2ptr(lsn, is_ext);
}

LSN sm_log_impl::ptr2lsn(fat_ptr ptr) { return _lm._lm.ptr2lsn(ptr); }

LSN sm_log::cur_lsn() {
  auto *log = &get_impl(this)->_lm;
  auto offset = log->cur_lsn_offset();
  auto *sid = log->_lm.get_offset_segment(offset);

  if (not sid) {
  /* must have raced a new segment opening */
  /*
  while (1) {
          sid = log->_lm._newest_segment();
          if (sid->start_offset >= offset)
                  break;
  }
  */

  retry:
    sid = log->_lm._newest_segment();
    ASSERT(sid);
    if (offset < sid->start_offset)
      offset = sid->start_offset;
    else if (sid->end_offset <= offset) {
      goto retry;
    }
  }
  return sid->make_lsn(offset);
}

void sm_log::dequeue_committed_xcts(uint64_t upto, uint64_t end_time) {
  LOG_IF(FATAL, !config::command_log) << "For command logging only";
  auto *log = &get_impl(this)->_lm;
  log->dequeue_committed_xcts(upto, end_time);
}

LSN sm_log::durable_flushed_lsn() {
  auto *log = &get_impl(this)->_lm;
  auto offset = log->dur_flushed_lsn_offset();
  auto *sid = log->_lm.get_offset_segment(offset);
  ASSERT(!sid || sid->start_offset <= offset);

  if (!sid) {
  retry:
    sid = log->_lm._newest_segment();
    ASSERT(sid);
    if (offset < sid->start_offset) {
      offset = sid->start_offset;
    } else if (sid->end_offset <= offset) {
      goto retry;
    }
  }
  ASSERT(sid);
  ASSERT(sid->start_offset <= offset);
  return sid->make_lsn(offset);
}

uint64_t sm_log::durable_flushed_lsn_offset() {
  return get_impl(this)->_lm.dur_flushed_lsn_offset();
}

void sm_log::wait_for_durable_flushed_lsn_offset(uint64_t offset) {
  auto *self = get_impl(this);
  self->_lm.wait_for_durable(offset);
}
}  // namespace ermia
