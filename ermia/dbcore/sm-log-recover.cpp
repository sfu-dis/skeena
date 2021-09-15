#include "rcu.h"
#include "sm-chkpt.h"
#include "sm-log-recover.h"
#include "sm-log-recover-impl.h"
#include "sm-log-impl.h"
#include "sm-oid.h"

#include <cstring>
#include <unistd.h>

namespace ermia {

sm_log_recover_mgr::sm_log_recover_mgr(sm_log_recover_impl *rf, void *rf_arg)
    : sm_log_offset_mgr(),
      scanner(new sm_log_scan_mgr_impl{this}),
      backup_replayer(nullptr),
      recover_functor(rf),
      backup_replay_functor(nullptr),
      recover_functor_arg(rf_arg) {
  LSN dlsn = get_durable_mark();
  bool changed = false;
  for (block_scanner it(this, dlsn, false, true, false); it.valid(); ++it) {
    // durable up to this point, but maybe no further
    if (it->checksum != it->full_checksum()) break;

    dlsn = it->next_lsn();
    changed = true;
  }

  if (changed) update_durable_mark(dlsn);

  auto *sid = get_segment(dlsn.segment());
  if (config::is_backup_srv()) {
    if (config::replay_threads > 0 && config::replay_policy != config::kReplayNone) {
      if (config::log_ship_offset_replay) {
        backup_replay_functor = new parallel_offset_replay;
      } else {
        backup_replay_functor = new parallel_oid_replay(config::replay_threads);
      }
      backup_replayer = new sm_log_scan_mgr_impl{this};
    }
  } else {
    truncate_after(sid->segnum, dlsn.offset());
  }
}

void sm_log_recover_mgr::recover() {
  if (!sm_log::need_recovery && !config::is_backup_srv()) {
    LOG(INFO) << "No need for recovery";
    return;
  }
  LSN chkpt_lsn = get_chkpt_start();
  if (chkpt_lsn.offset()) {
    sm_chkpt_mgr::recover(chkpt_lsn);
  }

  LOG(INFO) << "Will recover till " << std::hex << get_durable_mark().offset();
  {
    util::scoped_timer t("log_recovery", config::verbose);
    redo_log(chkpt_lsn, get_durable_mark());  // till end of log
  }
}

void sm_log_recover_mgr::redo_log(LSN start_lsn, LSN end_lsn) {
  (*recover_functor)(recover_functor_arg, scanner, start_lsn, end_lsn);
}

LSN sm_log_recover_mgr::backup_redo_log_by_oid(LSN start_lsn, LSN end_lsn) {
  return (*backup_replay_functor)(recover_functor_arg, backup_replayer, start_lsn, end_lsn);
}

void sm_log_recover_mgr::start_logbuf_redoers() {
  (*backup_replay_functor)(recover_functor_arg, backup_replayer, INVALID_LSN, INVALID_LSN);
}

sm_log_recover_mgr::~sm_log_recover_mgr() { delete scanner; }

sm_log_recover_mgr::block_scanner::block_scanner(sm_log_recover_mgr *lm,
                                                 LSN start,
                                                 bool follow_overflow,
                                                 bool fetch_payloads,
                                                 bool force_fetch_from_logbuf)
    : _lm(lm),
      _follow_overflow(follow_overflow),
      _fetch_payloads(fetch_payloads),
      _force_fetch_from_logbuf(force_fetch_from_logbuf) {
  int err = posix_memalign((void **)&_buf, DEFAULT_ALIGNMENT, fetch_payloads ? MAX_BLOCK_SIZE : MIN_BLOCK_FETCH);
  LOG_IF(FATAL, err != 0);
  _load_block(start, _follow_overflow);
}

sm_log_recover_mgr::block_scanner::~block_scanner() { free(_buf); };

void sm_log_recover_mgr::block_scanner::operator++() {
  /* If there are any remembered overflow blocks, process them
     first. We already chased their overflow chain (if any) so don't
     follow them the second time around.
   */
  if (_overflow_chain.empty()) {
    _load_block(_cur_block->next_lsn(), _follow_overflow);
  } else {
    _load_block(_overflow_chain.back(), false);
    _overflow_chain.pop_back();
  }
}

void sm_log_recover_mgr::block_scanner::_load_block(LSN x,
                                                    bool follow_overflow) {
  DEFER_UNLESS(success, *_buf = invalid_block());
  if (x == INVALID_LSN) return;

  /* WARNING: if we attempt to read past the end of the log, we'll
     ask for a segment that doesn't exist, causing assertion
     failures in get_segment(). However, we don't have to worry
     about race conditions during startup, so we can use a simpler
     segment lookup instead.
  */
  auto segnum = x.segment();
  auto *sid = _lm->segments[segnum];
  if (not sid) {
    sid = _lm->_newest_segment();
    if (sid->segnum % NUM_LOG_SEGMENTS != segnum % NUM_LOG_SEGMENTS) return;
  }

  bool read_from_logbuf = false;
  if (config::is_backup_srv()) {
    read_from_logbuf = _force_fetch_from_logbuf;
  } else {
    read_from_logbuf =
        logmgr && x.offset() > logmgr->durable_flushed_lsn_offset();
  }

  // helper function... pread may not read everything in one call
  auto pread = [&](size_t nbytes, uint64_t i) -> size_t {
    uint64_t offset = sid->offset(x);
    // See if it's stepping into the log buffer
    if (read_from_logbuf) {
      ALWAYS_ASSERT(config::is_backup_srv());
      // we should be scanning and replaying the log at a backup node
      auto *logbuf = logmgr->get_logbuf();
      // the caller should have called advance_writer(end_lsn) already
      if (logbuf->read_end() <= sid->buf_offset(x)) {
        return 0;
      }
      nbytes = std::min(nbytes, logbuf->read_end() - sid->buf_offset(x));
      ALWAYS_ASSERT(nbytes);
      _cur_block = (log_block *)logbuf->read_buf(sid->buf_offset(x), nbytes);
      return nbytes;
    } else {
      // otherwise we're recovering from disk, assuming the possibly NVRAM
      // backed logbuf is already flushed, ie durable_flushed_lsn ==
      // durable_lsn.
      _cur_block = _buf;
      return i + os_pread(sid->fd, ((char *)_buf) + i, nbytes - i, offset + i);
    }
  };

  /* Fetch log header */
  _cur_block = nullptr;
  size_t n = pread(MIN_BLOCK_FETCH, 0);

  if (n < MIN_BLOCK_FETCH) {
    /* Not necessarily a problem: we could be at the end of a
       segment and most log records are smaller than max size,
       or we could be reading from the NVRAM-backed log buffer
       (ie replaying a shipped log buffer) and hit the end (n == 0).
     */
    if (n < MIN_LOG_BLOCK_SIZE) return;                    // truncate!
    if (n < log_block::size(_cur_block->nrec, 0)) return;  // truncate!

    // fall out: looks like we're OK
  }

  ALWAYS_ASSERT(_cur_block);
  // cursory validation to avoid buffer overflow
  if (x != _cur_block->lsn or _cur_block->nrec > MAX_BLOCK_RECORDS)
    return;  // corrupt ==> truncate

  if (follow_overflow) {
    log_record *r = _cur_block->records;
    if (r->type == LOG_OVERFLOW) {
      _overflow_chain.push_back(_cur_block->lsn);
      success = true;
      return _load_block(r->prev_lsn, true);
      /* ^^^
         We marked the operation as "successful" to keep the
         deferred invalidation from firing (which it would do
         *after* we the recursive call returns. If the recursive
         call fails, our "success" will merely pass the block
         back to the user in its already-invalided form
       */
    }
  }

  if (!read_from_logbuf && _fetch_payloads) {
    uint64_t bsize = (char *)_cur_block->payload_end() - (char *)_cur_block;
    if (bsize > MAX_BLOCK_SIZE) return;  // corrupt ==> truncate

    if (bsize > n) {
      if (pread(bsize, n) < bsize) return;  // truncate!
    }
  }

  // phew!
  success = true;
}

sm_log_recover_mgr::log_scanner::log_scanner(sm_log_recover_mgr *lm, LSN start,
                                             bool fetch_payloads,
                                             bool force_fetch_from_logbuf)
    : _bscan(lm, start, true, fetch_payloads, force_fetch_from_logbuf),
      _i(0),
      has_payloads(fetch_payloads) {
  _find_record();
}

void sm_log_recover_mgr::log_scanner::operator++() {
  ++_i;
  _find_record();
}

/* Ensure that the scan currently rests on a "normal" log record,
   incrementing the count and/or block if necessary.
 */
void sm_log_recover_mgr::log_scanner::_find_record() {
  while (_bscan.valid()) {
    if (not(_i < _bscan->nrec)) {
      ++_bscan;
      _i = 0;
    } else if (_bscan->records[_i].type & LOG_FLAG_HAS_LSN) {
      ++_i;
    } else {
      break;
    }
  }
}

void sm_log_recover_mgr::load_object_from_logbuf(char *buf, size_t bufsz,
                                                 fat_ptr ptr, int align_bits) {
  THROW_IF(ptr.asi_type() != fat_ptr::ASI_LOG, illegal_argument,
           "Source object not stored in the log");
  size_t nbytes = decode_size_aligned(ptr.size_code(), align_bits);
  THROW_IF(bufsz < nbytes, illegal_argument,
           "Source object too large for buffer (%zd needed, %zd available)",
           nbytes, bufsz);

  auto segnum = ptr.log_segment();
  ASSERT(segnum >= 0);

  auto *sid = get_segment(segnum);
  ASSERT(sid);
  ASSERT(ptr.offset() >= sid->start_offset);

  ALWAYS_ASSERT(config::is_backup_srv());
  auto *logbuf = logmgr->get_logbuf();
  size_t read_end = logbuf->read_end();
  size_t read_begin = logbuf->read_begin();
  uint64_t offset = sid->buf_offset(LSN::from_ptr(ptr));
  if (read_begin <= offset && read_end > offset + nbytes) {
    memcpy(buf, logbuf->_get_ptr(offset), nbytes);
    // Verify to make sure we didn't read garbage
    read_end = logbuf->read_end();
    read_begin = logbuf->read_begin();
    if (read_begin <= offset && read_end > offset + nbytes) {
      return;
    }
  }
  while (ptr.offset() + nbytes > logmgr->durable_flushed_lsn().offset()) {
  }
  load_object(buf, bufsz, ptr, align_bits);
}

void sm_log_recover_mgr::load_object(char *buf, size_t bufsz, fat_ptr ptr,
                                     int align_bits) {
  THROW_IF(ptr.asi_type() != fat_ptr::ASI_LOG, illegal_argument,
           "Source object not stored in the log");
  size_t nbytes = decode_size_aligned(ptr.size_code(), align_bits);
  THROW_IF(bufsz < nbytes, illegal_argument,
           "Source object too large for buffer (%zd needed, %zd available)",
           nbytes, bufsz);

  auto segnum = ptr.log_segment();
  ASSERT(segnum >= 0);

  auto *sid = get_segment(segnum);
  ASSERT(sid);
  ASSERT(ptr.offset() >= sid->start_offset);

  if (config::is_backup_srv() && config::persist_policy != config::kPersistAsync &&
    ptr.offset() + nbytes > logmgr->durable_flushed_lsn().offset()) {
    return load_object_from_logbuf(buf, bufsz, ptr, align_bits);
  }

  size_t m = os_pread(sid->fd, buf, nbytes, ptr.offset() - sid->start_offset);
  LOG_IF(FATAL, m != nbytes) << "Unable to read full object ("
    << nbytes << " bytes needed, " << m << " read) at " << std::hex << ptr.offset()
    << " ,durable offset " << logmgr->durable_flushed_lsn().offset() << std::dec;
}

fat_ptr sm_log_recover_mgr::load_ext_pointer(fat_ptr ext_ptr) {
  /* Fix up the pointer: change ASI_EXT to ASI_LOG, and size it for
     a fat_ptr instead of the referenced object
   */
  auto flags = ext_ptr.flags();
  flags &= ~(fat_ptr::ASI_EXT_FLAG | fat_ptr::SIZE_MASK);
  flags |= fat_ptr::ASI_LOG_FLAG;
  uint8_t sz = 0x1;
  fat_ptr p = fat_ptr::make(ext_ptr.offset(), sz, flags);
  ASSERT(decode_size(sz) == 1);
  static_assert(sizeof(fat_ptr) <= DEFAULT_ALIGNMENT, "Buffer size too small!");
  union LOG_ALIGN {
    char buf[DEFAULT_ALIGNMENT];
    fat_ptr rval;
  };

  load_object(buf, sizeof(buf), p);
  return rval;
}

/***********************************************************************
 * BEGIN definitions for header_scan and record_scan
 *
 * Several functions should be factored out into a common base class,
 * but that would require multiple inheritance, which would in turn
 * require invoking the abomination that is virtual inheritance
 * because we have a diamond inheritance pattern.
 *
 * Replicate the code instead, keeping function body pairs together to
 * discourage implementations from diverging where they should not.
 */
bool sm_log_scan_mgr::header_scan::valid() {
  return get_impl(this)->scan.valid();
}
bool sm_log_scan_mgr::record_scan::valid() {
  auto *impl = get_impl(this);
  bool valid = impl->scan.valid();
  if (valid and impl->just_one) {
    /* If the user asked for just one transaction, we reject any
       LSN larger than the starting block. The transaction's
       overflow blocks (if any) all have smaller LSN.
     */
    return impl->scan._bscan->lsn <= impl->start_lsn;
  }
  return valid;
}

LSN sm_log_scan_mgr::record_scan::block_lsn() {
  return get_impl(this)->scan._bscan->lsn;
}

// same!
void sm_log_scan_mgr::header_scan::next() { ++get_impl(this)->scan; }
void sm_log_scan_mgr::record_scan::next() { ++get_impl(this)->scan; }

static sm_log_scan_mgr::record_type get_type(log_record_type tp) {
  switch (tp) {
    case LOG_INSERT:
    case LOG_INSERT_EXT:
      return sm_log_scan_mgr::LOG_INSERT;
    case LOG_INSERT_INDEX:
      return sm_log_scan_mgr::LOG_INSERT_INDEX;

    case LOG_UPDATE:
    case LOG_UPDATE_EXT:
      return sm_log_scan_mgr::LOG_UPDATE;
    case LOG_UPDATE_KEY:
      return sm_log_scan_mgr::LOG_UPDATE_KEY;

    case LOG_DELETE:
      return sm_log_scan_mgr::LOG_DELETE;
    case LOG_ENHANCED_DELETE:
      return sm_log_scan_mgr::LOG_ENHANCED_DELETE;

    case LOG_RELOCATE:
      return sm_log_scan_mgr::LOG_RELOCATE;

    case LOG_FID:
      return sm_log_scan_mgr::LOG_FID;

    case LOG_NOP:
    case LOG_COMMENT:
    case LOG_OVERFLOW:
    case LOG_SKIP:
    case LOG_FAT_SKIP:
      throw illegal_argument("invalid record type");
    default:
      throw illegal_argument("unknown record type");
  }
}

// same!
sm_log_scan_mgr::record_type sm_log_scan_mgr::header_scan::type() {
  auto tp = get_impl(this)->scan->type;
  return get_type(tp);
}
sm_log_scan_mgr::record_type sm_log_scan_mgr::record_scan::type() {
  auto tp = get_impl(this)->scan->type;
  return get_type(tp);
}

// same!
FID sm_log_scan_mgr::header_scan::fid() { return get_impl(this)->scan->fid; }
FID sm_log_scan_mgr::record_scan::fid() { return get_impl(this)->scan->fid; }

// same!
OID sm_log_scan_mgr::header_scan::oid() { return get_impl(this)->scan->oid; }
OID sm_log_scan_mgr::record_scan::oid() { return get_impl(this)->scan->oid; }

static size_t get_payload_size(sm_log_recover_mgr::log_scanner &s) {
  if (not(s->type & LOG_FLAG_HAS_PAYLOAD)) return sm_log_scan_mgr::NO_PAYLOAD;

  size_t rval = decode_size_aligned(s->size_code, s->size_align_bits);
  if (not(s->type & LOG_FLAG_IS_EXT)) ASSERT(rval == s.payload_size());

  return rval;
}

// same!
size_t sm_log_scan_mgr::header_scan::payload_size() {
  auto *impl = get_impl(this);
  return get_payload_size(impl->scan);
}
size_t sm_log_scan_mgr::record_scan::payload_size() {
  auto *impl = get_impl(this);
  return get_payload_size(impl->scan);
}

LSN sm_log_scan_mgr::record_scan::payload_lsn() {
  auto *impl = get_impl(this);
  return impl->scan.payload_lsn();
}

static std::pair<fat_ptr, bool> get_payload_ptr(
    sm_log_recover_mgr *lm, sm_log_recover_mgr::log_scanner &s,
    bool follow_ext) {
  if (not(s->type & LOG_FLAG_HAS_PAYLOAD))
    return std::make_pair(NULL_PTR, false);

  LSN x = s.payload_lsn();
  if (s->type & LOG_FLAG_IS_EXT) {
    fat_ptr pext = lm->lsn2ptr(x, true);
    if (not follow_ext) return std::make_pair(pext, false);

    if (s.has_payloads) return std::make_pair(*(fat_ptr *)s.payload(), false);

    return std::make_pair(lm->load_ext_pointer(pext), false);
  }

  // return the address of our payload
  return std::make_pair(lm->lsn2ptr(x, false), true);
}

/* The bodies of these two methods differ in whether---and if so,
   how---they resolve ASI_EXT pointers.
 */
fat_ptr sm_log_scan_mgr::header_scan::payload_ptr(bool follow_ext) {
  auto *impl = get_impl(this);
  auto rval = get_payload_ptr(impl->lm, impl->scan, follow_ext);
  return rval.first;
}
fat_ptr sm_log_scan_mgr::record_scan::payload_ptr() {
  auto *impl = get_impl(this);
  auto rval = get_payload_ptr(impl->lm, impl->scan, true);
  return rval.first;
}

static bool load_object(sm_log_recover_mgr *lm,
                        sm_log_recover_mgr::log_scanner &s, fat_ptr &pdest,
                        char *buf, size_t bufsz) {
  THROW_IF(not(s->type & LOG_FLAG_HAS_PAYLOAD), illegal_argument,
           "Attempt to load from non-payload record");

  auto psize = get_payload_size(s);
  THROW_IF(bufsz < psize, illegal_argument,
           "Buffer too small (%zd bytes needed, %zd available)", psize, bufsz);

  auto tmp = get_payload_ptr(lm, s, true);
  pdest = tmp.first;
  auto atype = pdest.asi_type();
  ASSERT(atype != fat_ptr::ASI_EXT);
  if (atype != fat_ptr::ASI_LOG) return false;

  if (tmp.second and s.has_payloads)
    std::memcpy(buf, (char *)s.payload(), psize);
  else
    lm->load_object(buf, bufsz, pdest, s->size_align_bits);

  return true;
}

bool sm_log_scan_mgr::header_scan::load_object(fat_ptr &pdest, char *buf,
                                               size_t bufsz) {
  auto *impl = get_impl(this);
  return ermia::load_object(impl->lm, impl->scan, pdest, buf, bufsz);
}
void sm_log_scan_mgr::record_scan::load_object(char *buf, size_t bufsz) {
  fat_ptr dummy;
  auto *impl = get_impl(this);
  auto rval = ermia::load_object(impl->lm, impl->scan, dummy, buf, bufsz);
  THROW_IF(not rval, illegal_argument,
           "Request to load object not resident in the log");
}

sm_log_scan_mgr_impl::sm_log_scan_mgr_impl(sm_log_recover_mgr *lm) : lm(lm) {}

sm_log_header_scan_impl::sm_log_header_scan_impl(sm_log_recover_mgr *lm,
                                                 LSN start,
                                                 bool force_fetch_from_logbuf)
    : scan(lm, start, false, force_fetch_from_logbuf), lm(lm) {}
sm_log_record_scan_impl::sm_log_record_scan_impl(sm_log_recover_mgr *lm,
                                                 LSN start, bool just_one_tx,
                                                 bool fetch_payloads,
                                                 bool force_fetch_from_logbuf)
    : scan(lm, start, fetch_payloads, force_fetch_from_logbuf),
      lm(lm),
      start_lsn(start),
      just_one(just_one_tx) {}

sm_log_scan_mgr::header_scan *sm_log_scan_mgr::new_header_scan(
    LSN start, bool force_fetch_from_logbuf) {
  auto *self = get_impl(this);
  return (sm_log_header_scan_impl *)make_new(self->lm, start,
                                             force_fetch_from_logbuf);
}

sm_log_scan_mgr::record_scan *sm_log_scan_mgr::new_log_scan(
    LSN start, bool fetch_payloads, bool force_fetch_from_logbuf) {
  auto *self = get_impl(this);
  return (sm_log_record_scan_impl *)make_new(
      self->lm, start, false, fetch_payloads, force_fetch_from_logbuf);
}

sm_log_scan_mgr::record_scan *sm_log_scan_mgr::new_tx_scan(
    LSN start, bool force_fetch_from_logbuf) {
  auto *self = get_impl(this);
  return (sm_log_record_scan_impl *)make_new(self->lm, start, true, true,
                                             force_fetch_from_logbuf);
}

void sm_log_scan_mgr::load_object(char *buf, size_t bufsz, fat_ptr ptr,
                                  size_t align_bits) {
  get_impl(this)->lm->load_object(buf, bufsz, ptr, align_bits);
}

fat_ptr sm_log_scan_mgr::load_ext_pointer(fat_ptr ptr) {
  return get_impl(this)->lm->load_ext_pointer(ptr);
}
}  // namespace ermia
