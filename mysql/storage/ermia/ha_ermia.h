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

/** @file ha_ermia.h

    @brief
  The ha_ermia engine is a stubbed storage engine for ermia purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/ermia/ha_ermia.cc.

    @note
  Please read ha_ermia.cc before reading this file.
  Reminder: The ermia storage engine implements all methods that are
  *required* to be implemented. For a full list of all methods that you can
  implement, see handler.h.

   @see
  /sql/handler.h and /storage/ermia/ha_ermia.cc
*/
#pragma once

#include <sys/types.h>

#include "my_base.h" /* ha_rows */
#include "my_compiler.h"
#include "my_inttypes.h"
#include "sql/handler.h" /* handler */
#include "thr_lock.h"    /* THR_LOCK, THR_LOCK_DATA */

#include <ermia.h>

#define ERMIA_PREALLOC_ARENA_SIZE 200 * 1024 * 1024
#define ERMIA_MAX_NO_OF_ATTRIBUTES_IN_KEY 32
#define ERMIA_MAX_KEY_LENGTH 3500  // Same as innodb
#define ERMIA_QUERY_BUFFER_SIZE STRING_BUFFER_USUAL_SIZE * 5
#define ERMIA_RECORDS_IN_RANGE 2

/** @brief
  Ermia_share is a class that will be shared among all open handlers.
  This example implements the minimum of what you will probably need.
*/
class Ermia_share : public Handler_share {
 public:
  THR_LOCK lock;
  Ermia_share();
  ~Ermia_share() { thr_lock_delete(&lock); }
};

/** @brief
  Class definition for the storage engine
*/
class ha_ermia : public handler {
  THR_LOCK_DATA lock;        ///< MySQL lock
  Ermia_share *share;        ///< Shared lock info
  Ermia_share *get_share();  ///< Get the share

  // MYSQL_RES *stored_result;
  /**
    Array of all stored results we get during a query execution.
  */

  // TODO: This invovles memory copy now, need to fix up
  std::vector<uchar> encode_record();
  void get_key_and_length();
  bool m_mysql_has_locked;

 public:
  ha_ermia(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_ermia() {}

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const { return "ERMIA"; }

  /**
    Replace key algorithm with one supported by SE, return the default key
    algorithm for SE if explicit key algorithm was not provided.

    @sa handler::adjust_index_algorithm().
  */
  virtual enum ha_key_alg get_default_index_algorithm() const {
    return HA_KEY_ALG_BTREE;
  }
  virtual bool is_index_algorithm_supported(enum ha_key_alg key_alg) const {
    return key_alg == HA_KEY_ALG_BTREE;
  }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const {
    /*
      We are saying that this engine is just statement capable to have
      an engine that can only handle statement-based logging. This is
      used in testing.
    */
    return HA_BINLOG_STMT_CAPABLE;
  }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx MY_ATTRIBUTE((unused)),
                    uint part MY_ATTRIBUTE((unused)),
                    bool all_parts MY_ATTRIBUTE((unused))) const {
    // We need to return this option for MySQL to know our index can do range
    // select
    return HA_READ_RANGE;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_keys() const { return (MAX_KEY); }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_parts() const {
    return ERMIA_MAX_NO_OF_ATTRIBUTES_IN_KEY;
  }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const { return ERMIA_MAX_KEY_LENGTH; }

  uint max_supported_key_part_length(
    HA_CREATE_INFO *create_info MY_ATTRIBUTE((unused))) const {
    return ERMIA_MAX_KEY_LENGTH;
  }
  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  virtual double scan_time() {
    return (double)(stats.records + stats.deleted) / 20.0 + 10;
  }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  virtual double read_time(uint, uint, ha_rows rows) {
    return (double)rows / 20.0 + 1;
  }

  /*
    Everything below are methods that we implement in ha_ermia.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_ermia.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *table_def);  // required

  /** @brief
    We implement this in ha_ermia.cc; it's a required method.
  */
  int close(void);  // required

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf);

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data);

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf);

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */

  int index_init(uint keynr, bool sorted);

  int index_read_idx_map(uchar *buf, uint index, const uchar *search_key,
                         key_part_map keypart_map,
                         enum ha_rkey_function find_flag);

  int index_read_map(uchar *buf, const uchar *search_key,
                     key_part_map keypart_map, enum ha_rkey_function find_flag);

  // ha_rows ha_ermia::estimate_rows_upper_bound();

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf);

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf);

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf);

  /** @brief
    We implement this in ha_ermia.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf);

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan);  // required
  int rnd_end();
  int rnd_next(uchar *buf);             ///< required
  int rnd_pos(uchar *buf, uchar *pos);  ///< required
  void position(const uchar *record);   ///< required
  int info(uint);                       ///< required
  int extra(enum ha_extra_function operation);
  uint lock_count(void) const;
  int external_lock(THD *thd, int lock_type);  ///< required

  int delete_all_rows(void);
  ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
  int delete_table(const char *from, const dd::Table *table_def);
  int rename_table(const char *from, const char *to,
                   const dd::Table *from_table_def, dd::Table *to_table_def);
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def);  ///< required
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);  ///< required
  ermia::varstr *Encode(ermia::str_arena *arena, const uchar *buf, const uint len, bool encode_type);
  void Decode(uchar *buf, ermia::varstr &val);
  ermia::ConcurrentMasstreeIndex *get_ermia_index(TABLE *table_args, uint key);
  
  void get_full_table_name(char *buf);
  void get_full_index_name(char *buf, TABLE *table_arg, uint key);
};

static inline std::string ermia_index_name(const std::string &,
                                           const std::string &);
static inline std::string ermia_index_name(const std::string &,
                                           const std::string &,
                                           const std::string &);
