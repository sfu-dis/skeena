#pragma once

#include "../varstr.h"

#include "sm-log-defs.h"
#include "sm-log-alloc.h"

namespace ermia {

struct sm_log_impl : sm_log {
  sm_log_impl(sm_log_recover_impl *rf, void *rarg) : _lm(rf, rarg) {}

  /* Convert the given LSN into a fat_ptr that can be used to access
     the corresponding log record.
   */
  fat_ptr lsn2ptr(LSN lsn, bool is_ext);

  /* Convert a fat_ptr into the LSN it corresponds to.

     Throw illegal_argument if the pointer does not correspond to
     any LSN.
   */
  LSN ptr2lsn(fat_ptr ptr);

  sm_log_alloc_mgr _lm;
};

/* NOTE: This class is needed during recovery, so the implementation
   is in sm-log-recover.cpp, not sm-log.cpp.
 */
struct sm_log_scan_mgr_impl : sm_log_scan_mgr {
  sm_log_scan_mgr_impl(sm_log_recover_mgr *lm);

  sm_log_recover_mgr *lm;
};

/* NOTE: This class is needed during recovery, so the implementation
   is in sm-log-recover.cpp, not sm-log.cpp.
 */
struct sm_log_header_scan_impl : sm_log_scan_mgr::header_scan {
  sm_log_header_scan_impl(sm_log_recover_mgr *lm, LSN start,
                          bool force_fetch_from_logbuf);

  sm_log_recover_mgr::log_scanner scan;
  sm_log_recover_mgr *lm;
};

/* NOTE: This class is needed during recovery, so the implementation
   is in sm-log-recover.cpp, not sm-log.cpp.
 */
struct sm_log_record_scan_impl : sm_log_scan_mgr::record_scan {
  sm_log_record_scan_impl(sm_log_recover_mgr *lm, LSN start, bool just_one_tx,
                          bool fetch_payloads, bool force_fetch_from_logbuf);

  sm_log_recover_mgr::log_scanner scan;
  sm_log_recover_mgr *lm;

  LSN start_lsn;
  bool just_one;
};

/* This is the staging area for log record requests until they
   actually find a home in a proper log block. We do this because we
   want the transaction's CSN to be as late as possible, and so we
   can't just allocate a log block to put log records into.
 */
struct sm_tx_log_impl : sm_tx_log {
  sm_tx_log_impl(sm_log_impl *l)
      : _log(l),
        _nreq(0),
        _payload_bytes(0),
        _prev_overflow(INVALID_LSN),
        _commit_block(NULL) {}

  sm_log_impl *_log;
  size_t _nreq;  // includes a pending overflow record, if such is needed!!!
  size_t _payload_bytes;
  LSN _prev_overflow;
  log_allocation *_commit_block;

  void add_request(log_request const &req);

  void add_payload_request(log_record_type type, FID f, OID o, fat_ptr p,
                           int abits, fat_ptr *pdest);
  void spill_overflow();
  void enter_precommit();

  log_allocation *_install_commit_block(log_allocation *a);
  void _populate_block(log_block *b);

  log_request log_requests[sm_log_recover_mgr::MAX_BLOCK_RECORDS];
};

DEF_IMPL(sm_log);
DEF_IMPL(sm_log_scan_mgr);
DEF_IMPL2(sm_log_scan_mgr::record_scan, sm_log_record_scan_impl);
DEF_IMPL2(sm_log_scan_mgr::header_scan, sm_log_header_scan_impl);
DEF_IMPL(sm_tx_log);

}  // namespace ermia
