#pragma once
#include <condition_variable>

#include <iostream>
#include <thread>
#include <vector>

#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/sendfile.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <thread>

#include "rdma.h"
#include "tcp.h"
#include "../macros.h"

#include "sm-chkpt.h"
#include "sm-config.h"
#include "sm-log.h"

namespace ermia {

struct write_record_t;

namespace rep {

class RdmaNode;

const uint64_t kRdmaWaiting = 0x1;
const uint64_t kRdmaReadyToReceive = 0x2;
const uint64_t kRdmaPersisted = 0x4;

extern uint64_t *global_persisted_lsn_ptr;
extern uint64_t replayed_lsn_offset;
extern uint64_t persisted_nvram_size;
extern uint64_t persisted_nvram_offset;
extern uint64_t new_end_lsn_offset;
extern std::condition_variable bg_replay_cond;
extern uint64_t received_log_size;
extern std::thread primary_async_ship_daemon;
extern std::condition_variable backup_shutdown_trigger;

static const uint32_t kMaxLogBufferPartitions = 64;
extern uint64_t log_redo_partition_bounds[kMaxLogBufferPartitions];
extern int replay_bounds_fd CACHE_ALIGNED;
extern std::vector<int> backup_sockfds;
extern std::mutex backup_sockfds_mutex;

extern std::mutex async_ship_mutex;
extern std::condition_variable async_ship_cond;

inline uint64_t GetReadView() {
  uint64_t lsn = 0;
  if (config::command_log) {
    return volatile_read(rep::replayed_lsn_offset);
  }
  if (config::persist_policy == config::kPersistAsync) {
    lsn = volatile_read(rep::replayed_lsn_offset);
  } else {
    lsn = std::min<uint64_t>(volatile_read(rep::replayed_lsn_offset),
                             volatile_read(*rep::global_persisted_lsn_ptr));
  }
  if (config::nvram_log_buffer) {
    lsn = std::min<uint64_t>(lsn, volatile_read(rep::persisted_nvram_offset));
  }
  return lsn;
}

struct backup_start_metadata {
  struct log_segment {
    segment_file_name file_name;
    uint64_t data_start;
    uint64_t size;
  };

  // Send over system config too, e.g, log buffer size.
  struct backup_config {
    uint32_t scale_factor;
    uint64_t log_segment_mb;
    uint32_t persist_policy;
    uint32_t command_log_buffer_mb;
    bool offset_replay;
  };

  struct backup_config system_config;
  char chkpt_marker[CHKPT_FILE_NAME_BUFSZ];
  char durable_marker[DURABLE_FILE_NAME_BUFSZ];
  char nxt_marker[NXT_SEG_FILE_NAME_BUFSZ];
  uint64_t chkpt_size;
  uint64_t log_size;
  uint64_t num_log_files;
  log_segment segments[0];  // must be the last one

  backup_start_metadata() : chkpt_size(0), log_size(0), num_log_files(0) {
    system_config.scale_factor = config::benchmark_scale_factor;
    system_config.log_segment_mb = config::log_segment_mb;
    system_config.offset_replay = config::log_ship_offset_replay;
    system_config.persist_policy = config::persist_policy;
    system_config.command_log_buffer_mb = config::command_log ?
                                          config::command_log_buffer_mb : 0;
  }

  inline void add_log_segment(unsigned int segment, uint64_t start_offset,
                              uint64_t end_offset, uint64_t data_start, uint64_t size) {
    // The filename; start_offset should already be adjusted according to
    // chkpt_start
    // so we only ship the part needed
    new (&(segments[num_log_files].file_name))
        segment_file_name(segment, start_offset, end_offset);

    // The real size we're going to send
    segments[num_log_files].size = size;

    // The actual offset in the file that will have log data
    segments[num_log_files].data_start = data_start;

    ++num_log_files;
    log_size += size;
  }
  inline uint64_t size() {
    return sizeof(*this) + sizeof(log_segment) * num_log_files;
  }
  inline log_segment* get_log_segment(uint32_t idx) { return &segments[idx]; }
  void persist_marker_files() {
    ALWAYS_ASSERT(config::is_backup_srv());
    // Write the marker files
    dirent_iterator dir(config::log_dir.c_str());
    int dfd = dir.dup();
    int marker_fd = os_openat(dfd, chkpt_marker, O_CREAT | O_WRONLY);
    os_close(marker_fd);
    marker_fd = os_openat(dfd, durable_marker, O_CREAT | O_WRONLY);
    os_close(marker_fd);
    marker_fd = os_openat(dfd, nxt_marker, O_CREAT | O_WRONLY);
    os_close(marker_fd);
  }
};

inline backup_start_metadata* allocate_backup_start_metadata(
    uint64_t nlogfiles) {
  uint32_t size = sizeof(backup_start_metadata) +
                  nlogfiles * sizeof(backup_start_metadata::log_segment);
  backup_start_metadata* md = (backup_start_metadata*)malloc(size);
  return md;
}

// Wait until the log buffer space up to [target_lsn] becomes available
// for receiving write from the primary.
inline void WaitForLogBufferSpace(LSN target_lsn) {
  // Make sure the half we're about to use is free now, i.e., data persisted
  // and replayed (if needed).
  uint64_t off = target_lsn.offset();
  if (off) {
    while (off > logmgr->durable_flushed_lsn().offset()) {
    }
    if (config::replay_policy != config::kReplayNone &&
        config::replay_policy != config::kReplayBackground) {
      while (off > volatile_read(replayed_lsn_offset)) {
      }
    }

    // Really make room for the incoming data.
    // Note: No CC for window buffer's advance_reader/writer. The backup
    // daemon is the only one that conduct these operations.
    // advance_writer is done when we receive the data right away.
    segment_id* sid = logmgr->get_segment(target_lsn.segment());
    sm_log::logbuf->advance_reader(sid->buf_offset(off));
  }
}

struct ReplayPipelineStage {
  LSN start_lsn;
  LSN end_lsn;
  uint64_t log_redo_partition_bounds[kMaxLogBufferPartitions];
  std::atomic<bool> consumed[kMaxLogBufferPartitions];
  std::atomic<uint32_t> num_replaying_threads;
  ReplayPipelineStage() : start_lsn(INVALID_LSN),
                          end_lsn(INVALID_LSN),
                          num_replaying_threads(0) {
    memset(log_redo_partition_bounds, 0, sizeof(uint64_t) * kMaxLogBufferPartitions);
  }
};
extern ReplayPipelineStage *pipeline_stages;

void BackupProcessLogData(ReplayPipelineStage &stage, LSN start_lsn, LSN end_lsn);
void start_as_primary();
void BackupStartReplication();
void primary_ship_log_buffer_all(const char* buf, uint32_t size, bool new_seg,
                                 uint64_t new_seg_start_offset);
backup_start_metadata* prepare_start_metadata(int& chkpt_fd,
                                              LSN& chkpt_start_lsn);
void PrimaryAsyncShippingDaemon();
void PrimaryShutdown();
void LogFlushDaemon();
void TruncateFilesInLogDir(); 

// RDMA-specific functions
void BackupDaemonRdma();
void PrimaryShutdownRdma();
void primary_daemon_rdma();
void start_as_backup_rdma();
void primary_init_rdma();
void primary_ship_log_buffer_rdma(const char* buf, uint32_t size, bool new_seg,
                                  uint64_t new_seg_start_offset);
void send_log_files_after_rdma(RdmaNode* node, backup_start_metadata* md);
void primary_rdma_poll_send_cq(uint64_t nops);
void primary_rdma_wait_for_message(uint64_t msg, bool reset);
void primary_rdma_set_global_persisted_lsn(uint64_t lsn);

// TCP-specific functions
void start_as_backup_tcp();
void BackupDaemonTcp();
void BackupDaemonTcpCommandLog();
void primary_daemon_tcp();
void send_log_files_after_tcp(int backup_fd, backup_start_metadata* md);
void PrimaryShutdownTcp();

/* Send a chunk of log records (still in memory log buffer) to a backup via TCP.
 */
void primary_ship_log_buffer_tcp(const char* buf, uint32_t size);
}  // namespace rep
}  // namespace ermia
