#!/usr/bin/env sysbench

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

-- ----------------------------------------------------------------------
-- TPCC-like workload
-- ----------------------------------------------------------------------

require("inno-tpcc_common")
require("inno-tpcc_run")
require("tpcc_check")

function thread_init(thread_id)
   drv,con=db_connection_init()
   trx_type = sysbench.opt.transaction_type
   abort_counter = 0
  -- debug info for payment
   wh_updated = false
   void_id = ""
  -- end debug info
   if thread_id == 0 then
      if trx_type == -1 then
        trx="mixed workload"
      elseif trx_type <= 10 then
        trx="new_order"
      elseif trx_type <= 20 then
        trx="payment"
      elseif trx_type <= 21 then
        trx="orderstatus"
      elseif trx_type <= 22 then
        trx="delivery"
      elseif trx_type <= 23 then
        trx="stocklevel"
      elseif trx_type <= 999 then
        trx="get_warehouse_id"
      end
      print(string.format("Running TPCC with workload %s", trx))
   end
   if sysbench.opt.debug == true then
   	if sysbench.opt.pin_warehouse then
   	        print("WAREHOUSE PINNING ENABLED")
   	end
    print(string.format("Engine mapping: %s", sysbench.opt.engine_mapping))
   	for key,value in pairs(sysbench) do
   	        print(string.format("Key: %s, Value: %s", key, value))
   	end
   end
end

-- function done()
--   print("done")
-- end

function event()
  -- print( NURand (1023,1,3000))
  local max_trx = 23
  local trx_type = sysbench.rand.uniform(1,max_trx)
  if sysbench.opt.transaction_type > -1 then
      trx_type = sysbench.opt.transaction_type
  end
  -- XXX: Fixed the benchmark to run NewOrder only transaction
  -- NewOrder (Works)
  -- Payment (Not working)
  -- OrderStatus (Not working)
  -- Delivery (Works)
  -- StockLevel (Works) -- Works but very slow, and a lot of aborts?
  if trx_type <= 10 then
    trx="new_order"
  elseif trx_type <= 20 then
    trx="payment"
  elseif trx_type <= 21 then
    trx="orderstatus"
  elseif trx_type <= 22 then
    trx="delivery"
    -- trx="stocklevel"
  elseif trx_type <= 23 then
    trx="stocklevel"
  elseif trx_type <= 999 then
    trx="get_warehouse_id"
  end

-- Execute transaction
   _G[trx]()

end

function sysbench.hooks.before_restart_event(err)
  con:query("ROLLBACK")
end

function sysbench.hooks.report_intermediate(stat)
   if  sysbench.opt.report_csv == "yes" then
   	sysbench.report_csv(stat)
   else
   	sysbench.report_default(stat)
   end
end

-- function sysbench.hooks.sql_error_ignorable(err)
--   print(err)
-- end

-- vim:ts=4 ss=4 sw=4 expandtab
