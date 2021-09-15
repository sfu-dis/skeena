# Run TPCC

## Prepare Tables

* Change the `scale` (# of warehouse) and `thread` accordingly in the `tpcc-prepare.sh` script
* Run `tpcc-prepare.sh <warehouses>` to build up the database
* Be sure to check that all tables are not empty afterwards

## Run benchmark

```
./tpcc.lua --mysql-socket=/dev/shm/void001/mysql/mysqld.sock --mysql-db=dbtest --scale=1 --threads=1 --mysql-storage-engine=InnoDB --use-fk=0 --report-interval=1 --pin-warehouse=false --transaction-type=-1 --time=30 --mysql-ignore-errors=1031 run
```

Some arguments in the command to be adjusted

* transaction-type: The type of TPC-C transaction
  - -1: Mixed, TPC-C default standard
  - 10: New Order
  - 20: Payment
  - 21: OrderStatus
  - 22: Delivery
  - 23: StockLevel

* mysql-ignore-errors: For the time being please at least add 1031 to the ignored errors list

* random-warehouse: Whether to use random warehouse or not, default true, if false, need to guarantee warehouse num >= thread num

* We support preparing table with different engine mapping the mapping order is as follows:
  1. warehouse
  2. district
  3. customer
  4. history
  5. orders
  6. new_orders
  7. order_line
  8. stock
  9. item
* For example, IIEIIIIII means only customer is in ERMIA, other tables are in InnoDB
