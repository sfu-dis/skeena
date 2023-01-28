-- Copyright (C) 2006-2018 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- -----------------------------------------------------------------------------
-- Common code for OLTP benchmarks.
-- -----------------------------------------------------------------------------

function init()
   assert(event ~= nil,
          "this script is meant to be included by other OLTP scripts and " ..
             "should not be called directly.")
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, warmup, run, " ..
            "cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
   table_size =
      {"Number of rows per table", 10000},
   range_size =
      {"Range size for range SELECT queries", 100},
   tables =
      {"Number of tables", 1},
   pgsql_percentage =
      {"Number of queries on PostgreSQL tables per transaction", 50},
   point_selects =
      {"Number of point SELECT queries per transaction", 10},
   simple_ranges =
      {"Number of simple range SELECT queries per transaction", 1},
   sum_ranges =
      {"Number of SELECT SUM() queries per transaction", 1},
   order_ranges =
      {"Number of SELECT ORDER BY queries per transaction", 1},
   distinct_ranges =
      {"Number of SELECT DISTINCT queries per transaction", 1},
   index_updates =
      {"Number of UPDATE index queries per transaction", 1},
   non_index_updates =
      {"Number of UPDATE non-index queries per transaction", 10},
   delete_inserts =
      {"Number of DELETE/INSERT combinations per transaction", 1},
   range_selects =
      {"Enable/disable all range SELECT queries", true},
   auto_inc =
   {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
       "or its alternatives in other DBMS. When disabled, use " ..
       "client-generated IDs", true},
   create_table_options =
      {"Extra CREATE TABLE options", ""},
   skip_trx =
      {"Don't start explicit transactions and execute all queries " ..
          "in the AUTOCOMMIT mode", false},
   secondary =
      {"Use a secondary index in place of the PRIMARY KEY", false},
   create_secondary =
      {"Create a secondary index in addition to the PRIMARY KEY", true},
   reconnect =
      {"Reconnect after every N events. The default (0) is to not reconnect",
       0},
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"},
   pgsql_variant =
      {"Use this PostgreSQL variant when running with the " ..
          "PostgreSQL driver. The only currently supported " ..
          "variant is 'redshift'. When enabled, " ..
          "create_secondary is automatically disabled, and " ..
          "delete_inserts is set to 0"}
}

function sleep(n)
   os.execute("sleep " .. tonumber(n))
end

-- XXX: ERMIA currently doesn't support concurrent DDL, in order to enable parallel loading,
-- we need to serially create all the tables in advance, i.e., before the prepare phase.
-- Use 1 thread to run the create phase.
function cmd_create()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables, 1 do
     create_table(drv, con, "pgsql", i)
     --- XXX: temporarily commented out
     ---create_table(drv, con, "ERMIA", i)
   end
end

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   sysbench.opt.threads do
     load_table(drv, con, "pgsql", i)
   end

     --- XXX: temporarily commented out
   ---sleep(10)
   ---
   ---for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   ---sysbench.opt.threads do
   ---  load_table(drv, con, "ERMIA", i)
   ---end
end

-- Preload the dataset into the server cache. This command supports parallel
-- execution, i.e. will benefit from executing with --threads > 1 as long as
-- --tables > 1
--
-- PS. Currently, this command is only meaningful for MySQL/InnoDB benchmarks
function cmd_warmup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   assert(drv:name() == "mysql", "warmup is currently MySQL only")

   -- Do not create on disk tables for subsequent queries
   con:query("SET tmp_table_size=2*1024*1024*1024")
   con:query("SET max_heap_table_size=2*1024*1024*1024")

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   sysbench.opt.threads do
      local t = "sbtest" .. i
      print("Preloading table " .. t)
      con:query("ANALYZE TABLE sbtest" .. i)
      con:query(string.format(
                   "SELECT AVG(id) FROM " ..
                      "(SELECT * FROM %s FORCE KEY (PRIMARY) " ..
                      "LIMIT %u) t",
                   t, sysbench.opt.table_size))
      con:query(string.format(
                   "SELECT COUNT(*) FROM " ..
                      "(SELECT * FROM %s WHERE k LIKE '%%0%%' LIMIT %u) t",
                   t, sysbench.opt.table_size))
   end
end

-- Implement parallel prepare and warmup commands, define 'prewarm' as an alias
-- for 'warmup'
sysbench.cmdline.commands = {
   create = {cmd_create, sysbench.cmdline.PARALLEL_COMMAND},
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
   warmup = {cmd_warmup, sysbench.cmdline.PARALLEL_COMMAND},
   prewarm = {cmd_warmup, sysbench.cmdline.PARALLEL_COMMAND}
}

function void_show_query(con, query)
    printf(query)
    -- con:query(query)
end

-- Template strings of random digits with 11-digit groups separated by dashes

-- 10 groups, 119 characters
local c_value_template = "###########-###########-###########-" ..
   "###########-###########-###########-" ..
   "###########-###########-###########-" ..
   "###########"

-- 5 groups, 59 characters
local pad_value_template = "###########-###########-###########-" ..
   "###########-###########"

function get_c_value()
   return sysbench.rand.string(c_value_template)
end

function get_pad_value()
   return sysbench.rand.string(pad_value_template)
end

function create_table(drv, con, engine, table_num)
   local id_index_def, id_def, secondary_index_def
   local engine_def = ""
   local query

   if sysbench.opt.secondary then
     id_index_def = "KEY xid"
   else
     id_index_def = "PRIMARY KEY"
     secondary_index_def = string.format("INDEX k_%d", table_num)
   end

   if drv:name() == "mysql"
   then
      if sysbench.opt.auto_inc then
         id_def = "INTEGER NOT NULL AUTO_INCREMENT"
      else
         id_def = "INTEGER NOT NULL"
      end
      engine_def = "/*! ENGINE = " .. engine .. " */"
   elseif drv:name() == "pgsql"
   then
      if not sysbench.opt.auto_inc then
         id_def = "INTEGER NOT NULL"
      elseif pgsql_variant == 'redshift' then
         id_def = "INTEGER IDENTITY(1,1)"
      else
         id_def = "SERIAL"
      end
   else
      error("Unsupported database driver:" .. drv:name())
   end

   print(string.format("Creating table 'sbtest%d_%s'...", table_num, engine))

   if sysbench.opt.create_secondary and engine ~= "ERMIA" then
      query = string.format([[
   CREATE TABLE sbtest%d_%s(
     id %s,
     k INTEGER DEFAULT '0' NOT NULL,
     c CHAR(120) DEFAULT '' NOT NULL,
     pad CHAR(60) DEFAULT '' NOT NULL,
     %s (id),
     %s (k)
   ) %s %s]],
      table_num, engine, id_def, id_index_def, secondary_index_def, engine_def,
      sysbench.opt.create_table_options)
   else
      query = string.format([[
   CREATE TABLE sbtest%d_%s(
     id %s,
     k INTEGER DEFAULT '0' NOT NULL,
     c CHAR(120) DEFAULT '' NOT NULL,
     pad CHAR(60) DEFAULT '' NOT NULL,
     %s (id)
   ) %s %s]],
      table_num, engine, id_def, id_index_def, engine_def,
      sysbench.opt.create_table_options)
   end
   
   con:query(query)
end

function load_table(drv, con, engine, table_num)
   local query

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into 'sbtest%d_%s'",
                          sysbench.opt.table_size, table_num, engine))
   end
   
   -- XXX : The maximum number of write-set entries in ERMIA is 256, so here we chop the original bulk insert into less bulky inserts.
   local part_count = math.ceil(sysbench.opt.table_size / 256)
   
   
   for part = 1, part_count do
      if sysbench.opt.auto_inc then
         query = "INSERT INTO sbtest" .. table_num .. "_" .. engine .. "(k, c, pad) VALUES"
      else
         query = "INSERT INTO sbtest" .. table_num .. "_" .. engine .. "(id, k, c, pad) VALUES"
      end

      -- XXX : We explicitly "BEGIN" a transaction in order to avoid an assertion failure.
      con:query("BEGIN")
      
      con:bulk_insert_init(query)
      
      local c_val
      local pad_val
      local part_start
      local part_end
      
      part_start = (part - 1) * 256 + 1
      
      if part < part_count then
         part_end = part_start + 255
      else
	     part_end = sysbench.opt.table_size
      end

      for i = part_start, part_end do

         c_val = get_c_value()
         pad_val = get_pad_value()

         if (sysbench.opt.auto_inc) then
            query = string.format("(%d, '%s', '%s')",
                               sysbench.rand.default(1, sysbench.opt.table_size),
                               c_val, pad_val)
         else
            query = string.format("(%d, %d, '%s', '%s')",
                               i,
                               sysbench.rand.default(1, sysbench.opt.table_size),
                               c_val, pad_val)
         end
         
         con:bulk_insert_next(query)
      end
      
      con:bulk_insert_done()

      con:query("COMMIT")
   end

end

local t = sysbench.sql.type
local stmt_defs = {
   point_selects_pgsql ={
      "SELECT c FROM sbtest%u_pgsql WHERE id=?",
      t.INT},
   point_selects_ERMIA ={
      "SELECT c FROM sbtest%u_ERMIA WHERE id=?",
      t.INT},
   point_selects = {
      "SELECT c FROM sbtest%u WHERE id=?",
      t.INT},
   simple_ranges = {
      "SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ?",
      t.INT, t.INT},
   sum_ranges = {
      "SELECT SUM(k) FROM sbtest%u WHERE id BETWEEN ? AND ?",
       t.INT, t.INT},
   order_ranges = {
      "SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c",
       t.INT, t.INT},
   distinct_ranges = {
      "SELECT DISTINCT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c",
      t.INT, t.INT},
   index_updates_pgsql = {
      "UPDATE sbtest%u_pgsql SET k=k+1 WHERE id=?",
      t.INT},
   index_updates_ERMIA = {
      "UPDATE sbtest%u_ERMIA SET k=k+1 WHERE id=?",
      t.INT},
   index_updates = {
      "UPDATE sbtest%u SET k=k+1 WHERE id=?",
      t.INT},
   non_index_updates_pgsql = {
      "UPDATE sbtest%u_pgsql SET c=? WHERE id=?",
      {t.CHAR, 120}, t.INT},
   non_index_updates_ERMIA = {
      "UPDATE sbtest%u_ERMIA SET c=? WHERE id=?",
      {t.CHAR, 120}, t.INT},
   non_index_updates = {
      "UPDATE sbtest%u SET c=? WHERE id=?",
      {t.CHAR, 120}, t.INT},
   deletes_pgsql = {
      "DELETE FROM sbtest%u_pgsql WHERE id=?",
      t.INT},
   deletes_ERMIA = {
      "DELETE FROM sbtest%u_ERMIA WHERE id=?",
      t.INT},
   deletes = {
      "DELETE FROM sbtest%u WHERE id=?",
      t.INT},
   inserts_pgsql = {
      "INSERT INTO sbtest%u_pgsql (id, k, c, pad) VALUES (?, ?, ?, ?)",
      t.INT, t.INT, {t.CHAR, 120}, {t.CHAR, 60}},
   inserts_ERMIA = {
      "INSERT INTO sbtest%u_ERMIA (id, k, c, pad) VALUES (?, ?, ?, ?)",
      t.INT, t.INT, {t.CHAR, 120}, {t.CHAR, 60}},
   inserts = {
      "INSERT INTO sbtest%u (id, k, c, pad) VALUES (?, ?, ?, ?)",
      t.INT, t.INT, {t.CHAR, 120}, {t.CHAR, 60}},
}

function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end

function prepare_for_each_table(key)
   for t = 1, sysbench.opt.tables do
      stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], t))

      local nparam = #stmt_defs[key] - 1

      if nparam > 0 then
         param[t][key] = {}
      end

      for p = 1, nparam do
         local btype = stmt_defs[key][p+1]
         local len

         if type(btype) == "table" then
            len = btype[2]
            btype = btype[1]
         end
         if btype == sysbench.sql.type.VARCHAR or
            btype == sysbench.sql.type.CHAR then
               param[t][key][p] = stmt[t][key]:bind_create(btype, len)
         else
            param[t][key][p] = stmt[t][key]:bind_create(btype)
         end
      end

      if nparam > 0 then
         stmt[t][key]:bind_param(unpack(param[t][key]))
      end
   end
end

function prepare_point_selects()
   prepare_for_each_table("point_selects_pgsql")
   --- XXX: temporarily commented out
   ---prepare_for_each_table("point_selects_ERMIA")
end

function prepare_simple_ranges()
   prepare_for_each_table("simple_ranges")
end

function prepare_sum_ranges()
   prepare_for_each_table("sum_ranges")
end

function prepare_order_ranges()
   prepare_for_each_table("order_ranges")
end

function prepare_distinct_ranges()
   prepare_for_each_table("distinct_ranges")
end

function prepare_index_updates()
   prepare_for_each_table("index_updates_pgsql")
   prepare_for_each_table("index_updates_ERMIA")
end

function prepare_non_index_updates()
   prepare_for_each_table("non_index_updates_pgsql")
   prepare_for_each_table("non_index_updates_ERMIA")
end

function prepare_delete_inserts()
   prepare_for_each_table("deletes_pgsql")
   prepare_for_each_table("inserts_pgsql")
   prepare_for_each_table("deletes_ERMIA")
   prepare_for_each_table("inserts_ERMIA")
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   -- Create global nested tables for prepared statements and their
   -- print("simple_ranges")
   -- parameters. We need a statement and a parameter set for each combination
   -- of connection/table/query
   stmt = {}
   param = {}

   for t = 1, sysbench.opt.tables do
      stmt[t] = {}
      param[t] = {}
   end

   -- This function is a 'callback' defined by individual benchmark scripts
   prepare_statements()
end

-- Close prepared statements
function close_statements()
   for t = 1, sysbench.opt.tables do
      for k, s in pairs(stmt[t]) do
         stmt[t][k]:close()
      end
   end
   if (stmt.begin ~= nil) then
      stmt.begin:close()
   end
   if (stmt.commit ~= nil) then
      stmt.commit:close()
   end
end

function thread_done()
   close_statements()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping table 'sbtest%d'...", i))
      con:query("DROP TABLE IF EXISTS sbtest" .. i )
   end
end

local function get_table_num()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function get_id()
   return sysbench.rand.default(1, sysbench.opt.table_size)
end

function begin()
   stmt.begin:execute()
end

function commit()
   stmt.commit:execute()
end

function execute_point_selects()
   local tnum_pgsql = get_table_num()
   local tnum_ERMIA = get_table_num()
   local i
   local pgsql_queries = math.ceil((sysbench.opt.pgsql_percentage / 100) * sysbench.opt.point_selects)
   local which_engine = {}
   
   for i = 1, pgsql_queries do
      which_engine[i] = 1
   end
   
   for i = pgsql_queries + 1, sysbench.opt.point_selects do
      which_engine[i] = 2
   end

   shuffle(which_engine)

   -- print("which engine starts")
   -- for i = 1, sysbench.opt.point_selects do
   --    print(which_engine[i])
   -- end
   -- print("which engine ends")

   for i = 1, sysbench.opt.point_selects do
      -- 1 is pgsql, 2 is ERMIA.
      if which_engine[i] == 1 then
         param[tnum_pgsql].point_selects_pgsql[1]:set(get_id())
         
         stmt[tnum_pgsql].point_selects_pgsql:execute()
      else
         param[tnum_ERMIA].point_selects_ERMIA[1]:set(get_id())
         
         stmt[tnum_ERMIA].point_selects_ERMIA:execute()
      end
   end
end

local function execute_range(key)
   local tnum = get_table_num()

   for i = 1, sysbench.opt[key] do
      local id = get_id()

      param[tnum][key][1]:set(id)
      param[tnum][key][2]:set(id + sysbench.opt.range_size - 1)

      stmt[tnum][key]:execute()
   end
end

function execute_simple_ranges()
   execute_range("simple_ranges")
end

function execute_sum_ranges()
   execute_range("sum_ranges")
end

function execute_order_ranges()
   execute_range("order_ranges")
end

function execute_distinct_ranges()
   execute_range("distinct_ranges")
end

function execute_index_updates()
   local tnum_pgsql = get_table_num()
   local tnum_ERMIA = get_table_num()
   local pgsql_queries = math.ceil((sysbench.opt.pgsql_percentage / 100) * sysbench.opt.index_updates)
   local which_engine = {}
   
   for i = 1, pgsql_queries do
      which_engine[i] = 1
   end
   
   for i = pgsql_queries + 1, sysbench.opt.index_updates do
      which_engine[i] = 2
   end

   shuffle(which_engine)

   for i = 1, sysbench.opt.index_updates do
      -- 1 is pgsql, 2 is ERMIA.
      if which_engine[i] == 1 then
         param[tnum_pgsql].index_updates_pgsql[1]:set(get_id())
         
         stmt[tnum_pgsql].index_updates_pgsql:execute()
      else
         param[tnum_ERMIA].index_updates_ERMIA[1]:set(get_id())
         
         stmt[tnum_ERMIA].index_updates_ERMIA:execute()
      end
   end
end

function execute_non_index_updates()
   local tnum_pgsql = get_table_num()
   local tnum_ERMIA = get_table_num()
   local pgsql_queries = math.ceil((sysbench.opt.pgsql_percentage / 100) * sysbench.opt.non_index_updates)
   local which_engine = {}
   
   for i = 1, pgsql_queries do
      which_engine[i] = 1
   end
   
   for i = pgsql_queries + 1, sysbench.opt.non_index_updates do
      which_engine[i] = 2
   end

   shuffle(which_engine)

   for i = 1, sysbench.opt.non_index_updates do
      -- 1 is pgsql, 2 is ERMIA.
      if which_engine[i] == 1 then
         param[tnum_pgsql].non_index_updates_pgsql[1]:set_rand_str(c_value_template)
         param[tnum_pgsql].non_index_updates_pgsql[2]:set(get_id())
         
         stmt[tnum_pgsql].non_index_updates_pgsql:execute()
      else
         param[tnum_ERMIA].non_index_updates_ERMIA[1]:set_rand_str(c_value_template)
         param[tnum_ERMIA].non_index_updates_ERMIA[2]:set(get_id())
         
         stmt[tnum_ERMIA].non_index_updates_ERMIA:execute()
      end
   end
end

function execute_delete_inserts()
   local tnum_pgsql = get_table_num()
   local tnum_ERMIA = get_table_num()
   local pgsql_queries = math.ceil((sysbench.opt.pgsql_percentage / 100) * sysbench.opt.delete_inserts)
   local which_engine = {}
   
   for i = 1, pgsql_queries do
      which_engine[i] = 1
   end
   
   for i = pgsql_queries + 1, sysbench.opt.delete_inserts do
      which_engine[i] = 2
   end

   shuffle(which_engine)

   for i = 1, sysbench.opt.delete_inserts do
      -- 1 is pgsql, 2 is ERMIA.
      local id = get_id()
      local k = get_id()
      
      if which_engine[i] == 1 then
         param[tnum_pgsql].deletes_pgsql[1]:set(id)

         param[tnum_pgsql].inserts_pgsql[1]:set(id)
         param[tnum_pgsql].inserts_pgsql[2]:set(k)
         param[tnum_pgsql].inserts_pgsql[3]:set_rand_str(c_value_template)
         param[tnum_pgsql].inserts_pgsql[4]:set_rand_str(pad_value_template)

         stmt[tnum_pgsql].deletes_pgsql:execute()
         stmt[tnum_pgsql].inserts_pgsql:execute()
      else
         param[tnum_ERMIA].deletes_ERMIA[1]:set(id)

         param[tnum_ERMIA].inserts_ERMIA[1]:set(id)
         param[tnum_ERMIA].inserts_ERMIA[2]:set(k)
         param[tnum_ERMIA].inserts_ERMIA[3]:set_rand_str(c_value_template)
         param[tnum_ERMIA].inserts_ERMIA[4]:set_rand_str(pad_value_template)

         stmt[tnum_ERMIA].deletes_ERMIA:execute()
         stmt[tnum_ERMIA].inserts_ERMIA:execute()
      end
   end
end

-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
   if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
      errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
      errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
      errdesc.sql_errno == 2011    -- CR_TCP_CONNECTION
   then
      close_statements()
      prepare_statements()
   end
end

function check_reconnect()
   if sysbench.opt.reconnect > 0 then
      transactions = (transactions or 0) + 1
      if transactions % sysbench.opt.reconnect == 0 then
         close_statements()
         con:reconnect()
         prepare_statements()
      end
   end
end

function shuffle(list)
   for i = #list, 2, -1 do
      local j = math.random(i)
      list[i], list[j] = list[j], list[i]
   end
end
