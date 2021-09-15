/* Copyright (c) 2019, 2020, Simon Fraser University. All rights reserved.
  Copyright (c) 2004, 2017, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_ermia.cc

  @brief
  The ha_ermia engine is a stubbed storage engine for ermia purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/ermia/ha_ermia.h.

  @details
  ha_ermia will let you create/open/delete tables, but
  nothing further (for example, indexes are not supported nor can data
  be stored in the table). Use this ermia as a template for
  implementing the same functionality in your own storage engine. You
  can enable the ermia storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-ermia-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE \<table name\> (...) ENGINE=ERMIA;

  The ermia storage engine is set up to use table locks. It
  implements an example "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  ermia handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_ermia.h before reading the rest
  of this file.

  @note
  When you create an ERMIA table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an example select that would do a scan of an entire
  table:

  @code
  ha_ermia::store_lock
  ha_ermia::external_lock
  ha_ermia::info
  ha_ermia::rnd_init
  ha_ermia::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::rnd_next
  ha_ermia::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_ermia::external_lock
  ha_ermia::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the ermia storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_ermia::open() would also have been necessary. Calls to
  ha_ermia::extra() are hints as to what will be occuring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#include <math.h>
#include <memory.h>
#include <atomic>
#include <iostream>
#include <string>
#include <stdlib.h>
#include "dbcore/rcu.h"
#include "dbcore/sm-log-recover-impl.h"
#include "dbcore/sm-rc.h"
#include "dbcore/sm-table.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/gtt.h"
#include "sql/handler.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/sql_parse.h"
#include "sql/sql_plugin.h"
#include "sql/sql_table.h"
#include "sql/table.h"
#include "typelib.h"

#include "include/my_base.h"
#include "include/my_sqlcommand.h"
#include "sql/sql_class.h"
#include "str_arena.h"
#include "txn.h"

#include "storage/ermia/debug_util.h"
#include "storage/ermia/ermia_callback.h"
#include "storage/ermia/ha_ermia.h"

#define ERMIA_DBUG_OFF

// ERMIA knobs
char *ermia_log_dir = nullptr;
char *ermia_tmpfs_dir = nullptr;
static uint64_t rc_cap = 0;
static uint64_t rc_sz = 0;
static bool rc_do = true;
static bool bypass_cross_container;
static bool commit_pipeline_is_on;

static MYSQL_SYSVAR_BOOL(ermia_commit_pipeline, commit_pipeline_is_on,
			 PLUGIN_VAR_RQCMDARG,
			 "Set this flag to false in order to disable commit pipeline", nullptr,
			 nullptr, true);
static MYSQL_SYSVAR_BOOL(ermia_htt, ermia::config::htt_is_on,
                         PLUGIN_VAR_RQCMDARG,
                         "Whether the hardware has HTT enabled", nullptr,
                         nullptr, true);
static MYSQL_SYSVAR_BOOL(ermia_bypass, bypass_cross_container,
                         PLUGIN_VAR_RQCMDARG,
                         "Set this flag to true in order to bypass cross-container transaction", nullptr,
                         nullptr, false);
static MYSQL_SYSVAR_BOOL(ermia_null_log_device, ermia::config::null_log_device,
                         PLUGIN_VAR_RQCMDARG,
                         "Whether to skip writing log records", nullptr,
                         nullptr, false);
static MYSQL_SYSVAR_BOOL(ermia_group_commit, ermia::config::group_commit,
                         PLUGIN_VAR_RQCMDARG,
                         "Wehther to use pipelined group commit", nullptr,
                         nullptr, false);
static MYSQL_SYSVAR_ULONG(ermia_node_memory_gb, ermia::config::node_memory_gb,
                          PLUGIN_VAR_RQCMDARG, "Per-node memory (GB)", nullptr,
                          nullptr, 4, 1, 1024, 1);
static MYSQL_SYSVAR_ULONG(ermia_log_segment_mb, ermia::config::log_segment_mb,
                          PLUGIN_VAR_RQCMDARG, "Log segment size in MB",
                          nullptr, nullptr, 8192, 8192, 65536, 8192);
static MYSQL_SYSVAR_STR(ermia_tmpfs_dir, ermia_tmpfs_dir, PLUGIN_VAR_RQCMDARG,
                        "Path to a tmpfs location. Used by log buffer", nullptr,
                        nullptr, "/dev/shm");
static MYSQL_SYSVAR_STR(ermia_log_dir, ermia_log_dir, PLUGIN_VAR_RQCMDARG,
                        "Log directory", nullptr, nullptr,
                        "/dev/shm/tzwang/ermia");
static MYSQL_SYSVAR_ULONG(gtt_recycle_capacity, rc_cap, PLUGIN_VAR_RQCMDARG,
                        "Tree capacity of a single index", nullptr, nullptr, 20, 1, ~uint64_t{0}, 1);

static MYSQL_SYSVAR_ULONG(gtt_recycle_threshold, rc_sz, PLUGIN_VAR_RQCMDARG,
                        "When the index tree count > <gtt_recycle_threshold>, it will perform the recycle process", 
                        nullptr, nullptr, 10, 1, ~uint64_t{0}, 1);
static MYSQL_SYSVAR_BOOL(gtt_do_recycle, rc_do, PLUGIN_VAR_RQCMDARG, "Wehther to use recycle", nullptr,
                         nullptr, true);
static SYS_VAR *ermia_system_variables[] = {MYSQL_SYSVAR(ermia_htt),
                                            MYSQL_SYSVAR(ermia_null_log_device),
                                            MYSQL_SYSVAR(ermia_group_commit),
                                            MYSQL_SYSVAR(ermia_node_memory_gb),
                                            MYSQL_SYSVAR(ermia_log_segment_mb),
                                            MYSQL_SYSVAR(ermia_tmpfs_dir),
                                            MYSQL_SYSVAR(ermia_log_dir),
                                            MYSQL_SYSVAR(ermia_bypass),
                                            MYSQL_SYSVAR(gtt_recycle_capacity),
                                            MYSQL_SYSVAR(gtt_recycle_threshold),
                                            MYSQL_SYSVAR(gtt_do_recycle),
					    MYSQL_SYSVAR(ermia_commit_pipeline),
                                            NULL};

static handler *ermia_create_handler(handlerton *hton, TABLE_SHARE *table,
                                     bool partitioned, MEM_ROOT *mem_root);

static int ermia_close_connection(handlerton *hton, THD *thd);
static void ermia_kill_connection(  handlerton *hton, THD *thd);
static int ermia_commit(handlerton *hton,  THD *thd, bool commit_trx);
static int ermia_abort(handlerton *hton, THD *thd, bool abort_trx);
int ermia_real_commit(handlerton *hton, THD *thd);

const int ERMIA_PREALLOC_ARENA_NUM = 500;
handlerton *ermia_hton = nullptr;
static ermia::Engine *ermia_engine = nullptr;
std::vector<std::atomic<ermia::str_arena *>> prealloc_arena(ERMIA_PREALLOC_ARENA_NUM);
std::atomic<int> next_arena(0);

/*
prealloc enough str_arena for trasactions
*/
void ermia_prealloc_arena() {
  for (int i = 0; i < ERMIA_PREALLOC_ARENA_NUM; i++) {
    prealloc_arena[i] = new ermia::str_arena(ermia::config::arena_size_mb);
  }
}

/*
wrap all transaction objects to put into THD
*/
struct ermia_thd {
 public:
  ermia::str_arena *arena;
  ermia::transaction *tx;
  int arena_idx;
  std::vector<ermia::varstr> result_vector;
  const uchar *query_key;
  int n_mysql_tables_in_use;
  int result_ptr;
  ermia::ConcurrentMasstree::ScanIterator<false> *scan_iter = nullptr;
  ermia::DirIterator dir_it;
  ermia_thd() : arena(nullptr), tx(nullptr), n_mysql_tables_in_use(0) {}
  ~ermia_thd() {}
};

/* Interface to mysqld, to check system tables supported by SE */
static bool ermia_is_supported_system_table(const char *db,
                                            const char *table_name,
                                            bool is_sql_layer_system_table);

Ermia_share::Ermia_share() { thr_lock_init(&lock); }

static int ermia_init_func(void *p) {
  DBUG_ENTER("ermia_init_func");

  const char rms[255] = "rm -rf %s/*";
  const char mks[255] = "mkdir -p %s";
  char buf[255] = "";
  sprintf(buf, rms, ermia_log_dir);
  system(buf);
  sprintf(buf, mks, ermia_log_dir);
  system(buf);

  ermia_hton = (handlerton *)p;
  ermia_hton->db_type = DB_TYPE_ERMIA;
  ermia_hton->state = SHOW_OPTION_YES;
  ermia_hton->create = ermia_create_handler;
  ermia_hton->flags = HTON_CAN_RECREATE;
  ermia_hton->is_supported_system_table = ermia_is_supported_system_table;
  ermia_hton->close_connection = ermia_close_connection;
  ermia_hton->kill_connection = ermia_kill_connection;
  ermia_hton->commit = ermia_commit;
  ermia_hton->rollback = ermia_abort;
  // XXX: Add knob for ermia handlerton
  // XXX: Add knob for commit pipeline
  enable_commit_pipeline = commit_pipeline_is_on;
  ermia::config::log_dir = std::string(ermia_log_dir);
  ermia::config::tmpfs_dir = std::string(ermia_tmpfs_dir);
  // FIXME: Hard-coded for now
  ermia::config::log_redo_partitions = 32;
  ermia::config::recover_functor = new ermia::parallel_oid_replay(1);
  ermia::config::arena_size_mb = 4;//
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

  ermia_hton->gtt = new GlobalTransactionTable(rc_cap);
  ermia_hton->gtt->setBypass(bypass_cross_container);
  ermia_hton->gtt->setCapacity(rc_cap);
  ermia_hton->gtt->setThreshold(rc_sz);
  ermia_hton->gtt->setDoRecycle(rc_do);

  ermia_prealloc_arena();
  ermia_engine = new ermia::Engine();
  DBUG_RETURN(0);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each ermia handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Ermia_share *ha_ermia::get_share() {
  Ermia_share *tmp_share;

  DBUG_ENTER("ha_ermia::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<Ermia_share *>(get_ha_share_ptr()))) {
    tmp_share = new Ermia_share;
    if (!tmp_share) {
      goto err;
    }
    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}


static int ermia_close_connection(handlerton *hton, THD *thd)
{
  DBUG_TRACE;
  DBUG_ASSERT(hton == ermia_hton);
  bool abort_trx = true;
  if (!thd_get_ha_data(thd, ermia_hton)) {
    return (0);
  }
  ermia_abort(hton, thd, abort_trx);
  return (0);
}

static void ermia_kill_connection(handlerton *hton,  THD *thd)
{
  DBUG_TRACE;
  DBUG_ASSERT(hton == ermia_hton);
  bool abort_trx = true;
  if (thd_get_ha_data(thd, ermia_hton)) {
    ermia_abort(hton, thd, abort_trx);
  } else {
    // No transaction in THD slot, don't do anything
  }
}

static handler *ermia_create_handler(handlerton *hton, TABLE_SHARE *table, bool,
                                     MEM_ROOT *mem_root) {
  return new (mem_root) ha_ermia(hton, table);
}

ha_ermia::ha_ermia(handlerton *hton, TABLE_SHARE *table_arg)
  : handler(hton, table_arg), m_mysql_has_locked(false) {}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_handler_tablename ha_ermia_system_tables[] = {
  {(const char *)NULL, (const char *)NULL}};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval true   Given db.table_name is supported system table.
    @retval false  Given db.table_name is not a supported system table.
*/
static bool ermia_is_supported_system_table(const char *db,
                                            const char *table_name,
                                            bool is_sql_layer_system_table) {
  st_handler_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table) {
    return false;
  }

  // Check if this is SE layer system tables
  systab = ha_ermia_system_tables;
  while (systab && systab->db) {
    if (systab->db == db && strcmp(systab->tablename, table_name) == 0) {
      return true;
    }
    systab++;
  }

  return false;
}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/


int ha_ermia::open(const char *, int, uint, const dd::Table *) {
  DBUG_ENTER("ha_ermia::open");

  if (!(share = get_share())) {
    DBUG_RETURN(1);
  }
  thr_lock_data_init(&share->lock, &lock, NULL);

  DBUG_RETURN(0);
}
/*
 * ERMIA also don't rely on the thr_lock, since ermia engine already handle the locks correctly
 */
uint ha_ermia::lock_count(void) const { return 0; }

/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_ermia::close(void) {
  DBUG_ENTER("ha_ermia::close");
  DBUG_RETURN(0);
}

void ha_ermia::get_full_table_name(char *buf) {
  memcpy(buf, table->s->db.str, table->s->db.length);
  buf[table->s->db.length] = '/';
  memcpy(buf + table->s->db.length + 1, table->s->table_name.str, table->s->table_name.length);
  buf[table->s->db.length + 1 + table->s->table_name.length] = '\0';
}

void ha_ermia::get_full_index_name(char *buf, TABLE *table_arg, uint key) {
  uint32_t idx = 0;
  memcpy(buf, table->s->db.str, table->s->db.length);
  idx += table->s->db.length;

  buf[idx] = '/';
  ++idx;

  memcpy(buf + idx, table->s->table_name.str, table->s->table_name.length);
  idx += table->s->table_name.length;

  buf[idx] = '/';
  ++idx;

  strcpy(buf + idx, table_arg->s->keynames.type_names[key]);
}

/**  @brief
   this is for debuging purpose, print the key or
   traverse all the fields and
   leverage the Key_map to determine whether a field is marked as key
**/
void ha_ermia::get_key_and_length() {
  int length = 0;
  for (Field **field = table->field; *field; field++) {
    if ((*field)->key_start.to_ulonglong() == 1) {
      length = (*field)->key_length();
    }
  }
}

int ha_ermia::write_row(uchar *buf) {
  DBUG_ENTER("ha_ermia::write_row");
  DBUG_ASSERT(buf != nullptr);

  ALWAYS_ASSERT(table);

  ermia_thd *et = (ermia_thd *)thd_get_ha_data(ha_thd(), ermia_hton);

  // FIXME: Handle the AUTO INCREMENT, we need to implement ha_ermia::index_last()
  if (table->next_number_field && buf == table->record[0]) {
    int ret = update_auto_increment();
    if (ret) {
      DLOG(ERROR) << "AUTO INCREMENT FAILURE, error: " << ret;
    }
  } 

  ermia::varstr *record = Encode(et->arena, buf, table->s->reclength, false);

  static const uint32_t kMaxName = 512;
  thread_local char full_table_name[kMaxName];
  
  // Insert to the table first to obtain an OID for the record
  get_full_table_name(full_table_name);
  ermia::TableDescriptor *td = ermia::TableDescriptor::Get(full_table_name);
  ALWAYS_ASSERT(td);

  ermia::transaction *txn = et->tx;
  // FIXME: The AUTO_INCREMENT does not have effect.
  ermia::OID oid = txn->Insert(td, record);

  // Note: in ERMIA this call (transaction::Insert) always succeeds
  ALWAYS_ASSERT(oid != ermia::INVALID_OID);

  // Handle indexes (if any) to set up key-OID mappings by traversing each
  // indexed field
  for (uint key = 0; key < table->s->keys; key++) {
    KEY *full_key = table->key_info + key; // key_info contains data of keys defined for the table
    uint key_len = 0;
    for (uint idx = 0; idx < full_key->actual_key_parts; idx++) {
      KEY_PART_INFO *key_part = full_key->key_part + idx;

      // Determine what is the length of the actual data (some variable length
      // data should include the length info) ref:
      // https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
      Field *field = key_part->field;
      key_len += field->data_length();
      if (field->type() == MYSQL_TYPE_VARCHAR) {
        key_len += HA_VARCHAR_PACKLENGTH(field->field_length);
      }
    }

    // Use the id generated by get_auto_increment as auto-incremented ID (key) 
    // if specified AUTO_INCREMENT
    ermia::varstr *insert_key = nullptr;

    // FIXME: How can we recycle this buf in this transaction.
    auto tmpbuf = et->arena->next(key_len)->data();
    auto off = 0u;
    for (uint idx = 0; idx < full_key->actual_key_parts; idx++) {
      auto field = full_key->key_part[idx].field;
      auto sz = field->data_length();
      if (field->type() == MYSQL_TYPE_VARCHAR) {
        sz += HA_VARCHAR_PACKLENGTH(field->field_length);
      }
      memcpy(tmpbuf + off, field->ptr, sizeof(uint8_t) * sz);
      off += sz;
    }

    insert_key = Encode(et->arena, reinterpret_cast<const uchar *>(tmpbuf), key_len, true);


    ermia::ConcurrentMasstreeIndex *ermia_idx = get_ermia_index(table, key);
    ALWAYS_ASSERT(ermia_idx);

    // Insert the full key first, then subkey
    if (!ermia_idx->InsertOID(txn, *insert_key, oid)) {
      // Key already exists - set OID array entry to point to nullptr (=deleted)
      txn->Update(td, oid, nullptr);
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    }

#if 0 
    // TODO: add upsert support
    // XXX: The following implementation does nothing (but add overhead) to the current
    // system, but we might want to make it work in the future
    // If we are inserting into the pk and pk has multiple fields (subkeys)
    if (0 /* Intentionally skip the code here */  && !strcmp("PRIMARY", full_key->name) && full_key->actual_key_parts > 1) {
        DLOG(INFO) << "Primary Index with multiple fields, insert all fields into index";
        for (auto idx = 0; idx < full_key->actual_key_parts; idx++) {

          // TODO: The logic here is a bit complicated. Need refine in the future
          // Copy each field value as the key and insert to corresponding index.
          auto field = full_key->key_part[idx].field;
          auto sz = field->data_length();
          if (field->type() == MYSQL_TYPE_VARCHAR) {
            sz += HA_VARCHAR_PACKLENGTH(field->field_length);
          }
          auto sub_insert_key = Encode(et->arena, reinterpret_cast<const uchar *>(field->ptr), sz, true);
          off += sz;
          auto sub_index_name = "sub/" + std::string(field->field_name);
          auto sub_idx = get_ermia_index(sub_index_name);
          ALWAYS_ASSERT(sub_idx);

          DLOG(INFO) << "Insert into subindex: " << sub_index_name;
          DLOG(INFO) << "Key: ";
          //printhex(reinterpret_cast<const char *>(sub_insert_key->data()), sub_insert_key->size());

          if (!sub_idx->InsertOID(txn, *sub_insert_key, oid)) {
            txn->Update(td, oid, nullptr);
            // TODO: Avoid this case
            DLOG(WARNING) << "Found dupp insertion, maybe this key is already inserted by a unique secondary index, skip for now";
          }
        }
    }
#endif

 }
  DBUG_RETURN(0);
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for ermia by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_ermia::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_ENTER("ha_ermia::update_row");
  MARK_REFERENCED(old_data);
  ha_statistic_increment(&System_status_var::ha_update_count);

  THD *thd = ha_thd();
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);

  const uchar *search_key = et->query_key;
  KEY orig_key = table->s->key_info[active_index];

  Field *field = orig_key.key_part[0].field;
  auto key_length = field->data_length();

  if (field->type() == MYSQL_TYPE_VARCHAR) {
    key_length += HA_VARCHAR_PACKLENGTH(field->field_length);
    key_length += *(reinterpret_cast<const uint16_t *>(search_key));
  } else {
    key_length = orig_key.key_length;
  }

  // FIXME: Keylenth is incorrect, for VARSTR
  ermia::varstr *key = Encode(et->arena, search_key, key_length, true);

  ermia::varstr *new_record = Encode(et->arena, new_data, table->s->reclength, false);
  ermia::ConcurrentMasstreeIndex *ermia_idx = get_ermia_index(table, active_index);

  // TODO: should we support no-index cases?
  auto rc = ermia_idx->UpdateRecord(et->tx, *key, *new_record);

  if (rc._val == RC_TRUE) {
    DBUG_RETURN(0);
  } else if (rc._val == RC_ABORT_INTERNAL) {
    // GetOID will return RC_FALSE, which gets translated to RC_ABORT_INTERNAL by
    // UpdateRecord (that uses GetOID) if key is not found or the corresponding
    // OID is INVALID_OID (deleted key)
    DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
  }

  // Otherwise it could be RC_ABORT_SI_CONFLICT or RC_ABORT_SERIAL returned by
  // transaction::Update. In either case, following first-updater-wins we
  // rollback this transaction.
  // Note : trans_rollback_stmt defined at sql/transaction.cc:564 which
  // should then call ha_rollback_trans; however in our case the "if" statement
  // at sql/transaction.cc:583 evaluates to false so ha_rollback_trans won't be
  // triggered, but this should be fine since we roll back everything in
  // ermia_abort anyway after external_lock detects
  // thd.transaction_rollback_request is true. A subsequent "commit" statement
  // though will not trigger a call to ermia_commit again (transaction already
  // concluded).
  thd->transaction_rollback_request = true;

  // TODO: return more proper error codes for SI_CONFLICT and SERIAL
  // errors. Note also that the RC_ABORT_* codes from ERMIA do *not* mean we
  // need to abort this transaction.
  if (rc._val == RC_ABORT_SI_CONFLICT) {
    DBUG_RETURN(HA_ERR_SI_CONFLICT);
  }

  DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_ermia::delete_row(const uchar *) {
  DBUG_ENTER("ha_ermia::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

// ha_rows ha_ermia::estimate_rows_upper_bound() { return HA_POS_ERROR; }

ermia::ConcurrentMasstreeIndex *ha_ermia::get_ermia_index(TABLE *table_args, uint key) {
  static const uint32_t kMaxName = 512;
  if (!strcmp(table_args->s->keynames.type_names[key], "PRIMARY")) {
    thread_local char full_table_name[kMaxName];
    get_full_table_name(full_table_name);
    ermia::TableDescriptor *td = ermia::TableDescriptor::Get(full_table_name);
    return (ermia::ConcurrentMasstreeIndex *)td->GetPrimaryIndex();
  }
  thread_local char full_index_name[kMaxName];
  get_full_index_name(full_index_name, table_args, key);
  return (ermia::ConcurrentMasstreeIndex *)ermia::TableDescriptor::GetIndex(full_index_name);
}

/*
 * buf:           The output buffer
 * search_key:    The key use to search
 * keypart_map:   The keypart to use
 * find_flag:     The direction / way to use the key (RANGE or POINT etc)
 *
 */
int ha_ermia::index_read_map(uchar *buf, const uchar *search_key,
                             key_part_map keypart_map,
                             enum ha_rkey_function find_flag) {
  DBUG_ENTER("ha_ermia::index_read_map");
  MARK_REFERENCED(keypart_map);

  THD *thd = ha_thd();
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  enum_sql_command sql_command = (enum_sql_command)thd_sql_command(thd);
  if (sql_command == SQLCOM_UPDATE) {
    et->query_key = search_key;
  }
  // Suppose this is the PRIMARY key
  // We'd better use active_index rather than keypart_map, which is mainly used
  // for the key with multiple fields
  KEY orig_key = table->s->key_info[active_index];

  Field *field = orig_key.key_part[0].field;
  auto key_length = field->data_length();
  if (field->type() == MYSQL_TYPE_VARCHAR) {
    key_length += HA_VARCHAR_PACKLENGTH(field->field_length);
    key_length += *(reinterpret_cast<const uint16_t *>(search_key));
  } else {
    key_length = orig_key.key_length;
  }

  ermia::varstr *key = Encode(et->arena, search_key, key_length, true);

  auto *idx = get_ermia_index(table, active_index);
  RangeScanCallback cb;
  rc_t rc;

  if (find_flag == HA_READ_KEY_EXACT) {
    // Exact read for unique index(including primary), we read only one record
    if (table->s->key_info[active_index].flags & HA_NOSAME) {
        ermia::varstr val;
        idx->GetRecord(et->tx, rc, *key, val, NULL);
        if (rc._val == RC_FALSE) {
          DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
        memcpy(buf, val.data(), val.size());
    } else {
        et->dir_it.reset();
        // Only read one key but we still have multiple records
        rc = idx->GetRecordMultiIt(et->tx, *key, &et->dir_it);
        bool eof;
        if(rc._val == RC_FALSE) {
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
        ALWAYS_ASSERT(et->dir_it.t != nullptr);
        auto first_result = et->dir_it.next(eof);
        if (eof) {
            DLOG(INFO) << "DirIterator EOF AT FIRST RECORD";
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
        memcpy(buf, first_result.data(), first_result.size());
    }
  } else {
    DLOG(INFO) << "scan_iterator: Construct";

    auto iter = ermia::ConcurrentMasstree::ScanIterator<false>::session_factory
               (&idx->GetMasstree(), et->tx->GetXIDContext(), *key, NULL);

    et->scan_iter = iter;

    // TODO: Write a callback ? maybe to figure out how to do range select.
    auto txn = et->tx;
    DLOG(INFO) << "scan_iterator: Initialize";
    bool more = iter->init_or_next<false>();
    if (!more) {
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    } else {
      DLOG(INFO) << "scan_iterator: Read 1st data";
      ermia::dbtuple *tuple = nullptr;
      tuple = ermia::oidmgr->oid_get_version(iter->tuple_array(), 
            	  iter->value(), txn->GetXIDContext());
      if (!tuple) {
        DBUG_RETURN(0);
      }

      ermia::varstr result;
      rc_t rc = txn->DoTupleRead(tuple, &result);
      DLOG(INFO) << "scan_iterator: Finish reading 1st data, result == " << rc._val;
      if (rc._val == RC_TRUE) {
        memcpy(buf, result.data(), result.size());
      }
    }
  }

  DBUG_RETURN(0);
}

int ha_ermia::index_read_idx_map(uchar *buf, uint index,
                                 const uchar *search_key,
                                 key_part_map keypart_map,
                                 enum ha_rkey_function find_flag) {
  DBUG_ENTER("ha_ermia::index_read_map");
  active_index = index;
  DBUG_RETURN(index_read_map(buf, search_key, keypart_map, find_flag));
}

int printhex(const char *data, int len) {
  int cnt = 0;
#ifdef DBUG_OFF
  MARK_REFERENCED(data);
#endif
  for (int i = 0; i < len; i++) {
    printf("%x%x ", ((unsigned int)data[i] & 0xF0) >> 4,
           (unsigned int)data[i] & 0x0F);
    cnt++;
    if (cnt == 8) {
      printf(" ");
    }
    if (cnt == 16) {
      printf("\n");
      cnt = 0;
    }
  }
  if (len % 16) printf("\n");
  return 0;
}

int ha_ermia::index_init(uint keynr, bool) {
  DBUG_ENTER("ha_ermia::index_init");
  DBUG_PRINT("info", ("table: '%s'  key: %u", table->s->table_name.str, keynr));
  active_index = keynr;
  DBUG_RETURN(0);
}

/**
  @brief
  Used to read forward through the index.
  */

int ha_ermia::index_next(uchar *data) {
  DBUG_ENTER("ha_ermia::index_next");

  THD *thd = ha_thd();
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  ALWAYS_ASSERT(et->scan_iter != nullptr || et->dir_it.t != nullptr);

  if (et->scan_iter != nullptr) {
    DLOG(INFO) << "scan_iterator: index_next_called";
    auto iter = et->scan_iter;
    DLOG(INFO) << "scan_iterator: next value";
    bool more = iter->init_or_next<true>();
    if (!more) {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    ermia::varstr result;
    auto txn = et->tx;
    ermia::dbtuple *tuple = ermia::oidmgr->oid_get_version(iter->tuple_array(), 
          	  iter->value(), txn->GetXIDContext());
    DLOG(INFO) << "scan_iterator: read next value, tuple = " << tuple;
    if (tuple) {
      rc_t rc = txn->DoTupleRead(tuple, &result);
      DLOG(INFO) << "scan_iterator: read next value data, result == " << rc._val;
      if (rc._val == RC_TRUE) {
        memcpy(data, result.data(), result.size());
        DBUG_RETURN(0);
      }
    }
  } else if (et->dir_it.t != nullptr) {
    bool eof;
    auto result = et->dir_it.next(eof);
    if (!eof) {
      /* align the data to 64 boundary */
      memcpy(data, result.data(), result.size());
      DBUG_RETURN(0);
    }
  }

  if (et->dir_it.t != nullptr) {
      et->dir_it.reset();
  }
  DBUG_RETURN(HA_ERR_END_OF_FILE);
}

/**
  @brief
  Used to read backwards through the index.
  */

int ha_ermia::index_prev(uchar *data) {
  int rc;
  DBUG_ENTER("ha_ermia::index_prev");
  MARK_REFERENCED(data);
  rc = HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}

/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
  */
int ha_ermia::index_first(uchar *data) {
  int rc;
  DBUG_ENTER("ha_ermia::index_first");
  MARK_REFERENCED(data);
  rc = HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(0);
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
  */
int ha_ermia::index_last(uchar *data) {
  DBUG_ENTER("ha_ermia::index_last");
  ALWAYS_ASSERT(table);

  THD *thd = ha_thd();
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  ermia::transaction *txn = et->tx;

  // Get keyinfo, we need the keylen for encoding
  auto pk_idx = table->s->primary_key;
  KEY* pk_info = table->s->key_info + pk_idx;
  auto key_len = pk_info->key_length;
  // FIXME: Suppose the index is PRIMARY is not correct.
  ermia::ConcurrentMasstreeIndex *ermia_idx = get_ermia_index(table, 0);//index_name);
  RangeScanCallback cb;
  uint32_t start_val = 0x0;
  ermia::varstr *start_key = Encode(et->arena, (const uchar *)&start_val, key_len, true);
  // TODO: Currently this will scan all the record from masstree and then we choose
  // the last record, which has performance issue, later we need to use ReverseScan and modify
  // its API to retrieve only first N result (then we can retrieve 1 record only)
  ermia_idx->Scan(txn, *start_key, nullptr, cb, et->arena);
  auto res = cb.result();

  if (!res.size()) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  // Fetch the last row
  auto val = res[res.size() - 1];
  memcpy(data, val.data(), val.size());
  DBUG_RETURN(0);
}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the ermia in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
  */
// FIXME: Use correct ordering for rnd_init (do a masstree scan)
int ha_ermia::rnd_init(bool) {
  DBUG_ENTER("ha_ermia::rnd_init");
  ALWAYS_ASSERT(table);

  static const uint32_t kMaxName = 512;
  thread_local char full_table_name[kMaxName];
  get_full_table_name(full_table_name);
  ermia::TableDescriptor *td = ermia::TableDescriptor::Get(full_table_name);
  THD *thd = ha_thd();
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  ermia::transaction *txn = et->tx;

  auto *alloc = ermia::oidmgr->get_allocator(td->GetTupleFid());
  ermia::OID himark = alloc->head.hiwater_mark;
  std::vector<ermia::varstr> records;
  std::vector<ermia::OID> oids;

  for(ermia::OID oid = 0; oid < himark; oid++) {
    ermia::fat_ptr ptr = ermia::oidmgr->oid_get(td->GetTupleArray(), oid);
    if(not ptr.offset()) {
      continue;
    }

    if(ptr.asi() & ermia::fat_ptr::ASI_DIR) {
       oids.clear();
       continue;
    } else {
        oids.clear();
        oids.push_back(oid);
    }

    for (auto &o : oids) {
        ermia::varstr val;
        auto *tuple = ermia::oidmgr->oid_get_version(td->GetTupleArray(), o, txn->GetXIDContext());
        if(tuple) {
          ermia::varstr val;
          txn->DoTupleRead(tuple, &val);
          records.push_back(val);
        }
    }
  }

  et->result_vector = records;
  et->result_ptr = 0;

  DBUG_RETURN(0);
}

int ha_ermia::rnd_end() {
  DBUG_ENTER("ha_ermia::rnd_end");
  DBUG_RETURN(0);
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
  */
int ha_ermia::rnd_next(uchar *record) {
  DBUG_ENTER("ha_ermia::rnd_next");

  THD *thd = ha_thd();
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  int record_index = et->result_ptr++;

  if (record_index >= (int)et->result_vector.size()) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  ermia::varstr result = et->result_vector[record_index];
  memcpy(record, result.data(), result.size());

  DBUG_RETURN(0);
}

/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
  */
void ha_ermia::position(const uchar *) {
  DBUG_ENTER("ha_ermia::position");
  DBUG_VOID_RETURN;
}

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and
  sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
  */
int ha_ermia::rnd_pos(uchar *, uchar *) {
  int rc;
  DBUG_ENTER("ha_ermia::rnd_pos");
  rc = HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really
  needed. SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
  records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
  records
  deleted
  data_file_length
  index_file_length
  delete_length
  check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc and sql_update.cc
  */
int ha_ermia::info(uint) {
  DBUG_ENTER("ha_ermia::info");
  DBUG_RETURN(0);
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

  @see
  ha_innodb.cc
  */
int ha_ermia::extra(enum ha_extra_function) {
  DBUG_ENTER("ha_ermia::extra");
  DBUG_RETURN(0);
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases
  where the optimizer realizes that all rows will be removed as a result of an
  SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
  */
int ha_ermia::delete_all_rows() {
  DBUG_ENTER("ha_ermia::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to
  understand this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
  */
int ha_ermia::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("ha_ermia::external_lock");
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  if (et == NULL) {
    et = new ermia_thd();
    int idx = -1;
    do {
      idx = next_arena.fetch_add(1) % ERMIA_PREALLOC_ARENA_NUM;
      et->arena = prealloc_arena[idx].exchange(nullptr);
    } while (!et->arena);
    et->arena_idx = idx;

    ermia::transaction *transaction_buffer =
      (ermia::transaction *)malloc(sizeof(ermia::transaction));
    et->tx = ermia_engine->NewTransaction(0, *et->arena, transaction_buffer);
    auto *xc = et->tx->GetXIDContext();
    if (thd->begin_ermia_lsn /* is valid */) {
      xc->begin = thd->begin_ermia_lsn;
    } else {
      // Only when the global txn touches innodb
      // we store the entry in global state table
      thd->begin_ermia_lsn = xc->begin;
    }
    thd->set_ha_data_ptr(ermia_hton, et);
    assert(thd_get_ha_data(thd, ermia_hton) == et);
  }

  if (lock_type != F_UNLCK) {
    /* MySQL is setting a new table lock */
    if (thd_test_options(
          thd, OPTION_BEGIN)) {  // register ermia engine in the THD so that
                                 // MySQL can call rollback() in ermia_hton
      trans_register_ha(thd, 1, ermia_hton, nullptr);
    }
    et->n_mysql_tables_in_use++;
    m_mysql_has_locked = true;
  } else {
    et->n_mysql_tables_in_use--;
    m_mysql_has_locked = false;
    if (et->n_mysql_tables_in_use == 0) {
      if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
        if (thd->transaction_rollback_request) {
          ermia_abort(ht, thd, true);
        } else {
          ermia_commit(ht, thd, true);
        }
      }
    }
  }
  DBUG_RETURN(0);
}

static void ermia_cleanup(handlerton *hton, THD *thd, bool is_abort) {
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  ermia::transaction *txn = et->tx;
  ALWAYS_ASSERT(txn);
  if (is_abort) {
    ermia_engine->Abort(txn);
  }
  ermia::str_arena *txn_arena = et->arena;
  txn_arena->reset();
  prealloc_arena[et->arena_idx].store(et->arena, std::memory_order_release);
  free(et->tx);

  if (et->dir_it.t != nullptr) {
      et->dir_it.reset();
  }
  delete et->scan_iter;
  et->scan_iter = nullptr;
  thd->set_ha_data_ptr(hton, nullptr);
}

int ermia_real_commit(handlerton *hton, THD *thd) {
  DBUG_ENTER("ermia_real_commit");
  DBUG_ASSERT(hton == ermia_hton);

  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  rc_t rc = ermia_engine->Commit(et->tx);

  if (rc._val == RC_TRUE) {
    uint64_t lsn = ermia_engine->GetLSN(et->tx);
    thd->rlsn.push(lsn, ermia::lsn_ermia);
  }

  // Under SI commit always succeeds
  ALWAYS_ASSERT(rc._val == RC_TRUE);

  // TODO: reconsider abort case here for higher isolation levels, below
  // commented out code follows innobase
  /*
  if (rc.IsAbort()){
    // same with DB_FORCE_ABORT in Innodb
    thd_mark_transaction_to_rollback(thd, 1);
  }
  */
  ermia_cleanup(hton, thd, rc.IsAbort());
  DBUG_RETURN(0);
}

static int ermia_commit(handlerton *hton, THD *thd, bool /* commit_trx */) {
  DBUG_ENTER("ermia_commit");
  DBUG_ASSERT(hton == ermia_hton);
  ermia_thd *et = (ermia_thd *)thd_get_ha_data(thd, ermia_hton);
  if (et->tx->state() == ermia::TXN::TXN_ABRTD) {
    ermia_cleanup(hton, thd, true);
    DBUG_RETURN(1);
  }

  uintptr_t commit_lsn = ermia_engine->PreCommit(et->tx);

  // TODO: Pass the pre_lsn to InnoDB
  current_thd->ermia_pre_commit_lsn = commit_lsn;

  // It's not a cross container or InnoDB transaction, we directly call ermia_real_commit
  if (!thd->inno_trx_id) {
    DBUG_RETURN(ermia_real_commit(hton, thd));
  }
  DBUG_RETURN(0);
}

static int ermia_abort(handlerton *hton, THD *thd, bool /* abort_trx */) {
  ermia_cleanup(hton, thd, true);
  return 0;
}

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for ermia, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
  */
THR_LOCK_DATA **ha_ermia::store_lock(THD *, THR_LOCK_DATA **to,
                                     enum thr_lock_type lock_type) {
  /* 
   * XXX: THR_LOCK can be really time consuming and may
   * further cause starvation problem (which already happen in our
   * TPC-C benchmark). This must be a performance bottleneck, MySQL
   * 5.7 has already dropped the THR_LOCK for InnoDB, and using InnoDB's
   * internal metalocks, since ERMIA is almost lock-free, we can ignore
   * the THR_LOCK as well, and thus prevent the starvation problem
   */
  return to;
}

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
  */

// FIXME: delete_table should delete all the table index, and the oid array
int ha_ermia::delete_table(const char *, const dd::Table *) {
  DBUG_ENTER("ha_ermia::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}

/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
  */
int ha_ermia::rename_table(const char *, const char *, const dd::Table *,
                           dd::Table *) {
  DBUG_ENTER("ha_ermia::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details

  end_key may be empty, in which case determine if start_key matches any rows.
  Called from opt_range.cc by check_quick_keys().
  @see
  check_quick_keys() in opt_range.cc
  */
ha_rows ha_ermia::records_in_range(uint, key_range *, key_range *) {
  DBUG_ENTER("ha_ermia::records_in_range");
  DBUG_RETURN(10);  // low number to force index usage
}

/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
  */

int ha_ermia::create(const char *tablename, TABLE *table_arg,
                     HA_CREATE_INFO *create_info, dd::Table *table_def) {
  DBUG_ENTER("ha_ermia::create");
  MARK_REFERENCED(tablename);
  MARK_REFERENCED(table_def);
  stats.auto_increment_value = create_info->auto_increment_value;
  
  static const uint32_t kMaxName = 512;
  thread_local char full_table_name[kMaxName];
  thread_local char full_index_name[kMaxName];
  
  // now we want only one indiraction array for one mysql-ermia table,
  // and one masstree for each index field (both primary or secondary).
  get_full_table_name(full_table_name);

  ermia_engine->CreateTable(full_table_name);

  for (uint key = 0; key < table_arg->s->keys; key++) {
    if (!strcmp(table->s->keynames.type_names[key], "PRIMARY")) {
      get_full_index_name(full_index_name, table_arg, key);
      ermia_engine->CreateMasstreePrimaryIndex(full_table_name, full_index_name);
    } else {
      bool is_unique = table->s->key_info[key].flags & HA_NOSAME;
      get_full_index_name(full_index_name, table_arg, key);
      ermia_engine->CreateMasstreeSecondaryIndex(full_table_name, full_index_name, is_unique);
    }
  }

  DBUG_RETURN(0);
}

struct st_mysql_storage_engine ermia_storage_engine = {
  MYSQL_HANDLERTON_INTERFACE_VERSION
};

ermia::varstr *ha_ermia::Encode(ermia::str_arena *arena, const uchar *buf, const uint len,
                                bool encode_type) {
  ermia::varstr *str = arena->next(len);
  new (str) ermia::varstr((char *)str + sizeof(ermia::varstr), len);
  if (!encode_type) {
    std::memcpy((void *)(str->p), buf, len);
  } else {
    if (len == 2) {
      int16_t i16 = ((int16_t)buf[0] & 0x00FF) | (((int16_t)buf[1] & 0x00FF) << 8);
      int16_t i16big = HOST_TO_BIG_TRANSFORM(int16_t, i16);
      std::memcpy((void *)(str->p), &i16big, sizeof(i16big));
    } else if (len == 4) {
      // FIXME: Why we need to do bitwise operation to get the int32? I think a direct conversion is okay
      int32_t i32 = ((int32_t)buf[0] & 0x000000FF) | (((int32_t)buf[1] & 0x000000FF) << 8) | (((int32_t)buf[2] & 0x000000FF) << 16) | (((int32_t)buf[3] & 0x000000FF) << 24);
      int32_t i32big = HOST_TO_BIG_TRANSFORM(int32_t, i32);
      std::memcpy((void *)(str->p), &i32big, sizeof(i32big));
    } else
      std::memcpy((void *)(str->p), buf, len);
  }

  return str;
}

mysql_declare_plugin(ermia) {
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &ermia_storage_engine,
  "ERMIA",
  "Tianzheng Wang",
  "ERMIA storage engine",
  PLUGIN_LICENSE_GPL,
  ermia_init_func, /* Plugin Init */
  NULL,            /* Plugin check uninstall */
  NULL,            /* Plugin Deinit */
  0x0001 /* 0.1 */,
  NULL,                   /* status variables */
  ermia_system_variables, /* system variables */
  NULL,                   /* config options */
  0,                      /* flags */
} mysql_declare_plugin_end;
