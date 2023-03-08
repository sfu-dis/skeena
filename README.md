## Skeena: Efficient and Consistent Cross-Engine Transactions

Skeena enables consistent ACID cross-engine transactions over two database engines (e.g., storage-centric InnoDB and memory-optimized ERMIA, both in MySQL) without intrusive changes to individual engines.

This repository implements [Skeena](https://www.cs.sfu.ca/~tzwang/skeena.pdf). See details in our [SIGMOD 2022 paper](https://www.cs.sfu.ca/~tzwang/skeena.pdf) below. If you use our work, please cite:

```
Skeena: Efficient and Consistent Cross-Engine Transactions.
Jianqiu Zhang, Kaisong Huang, Tianzheng Wang and King Lv.
ACM SIGMOD/PODS International Conference on Management of Data 2022
```

The implementation integrates the [ERMIA](https://github.com/sfu-dis/ermia) (SIGMOD 2016) main-memory database engine into MySQL alongside with its default InnoDB engine to enable cross-engine transactions.

### Directory structure and notable source files:
* mysql/ - modified MySQL to support ERMIA and Skeena
* mysql/storage/innobase/trx/trx0trx.cc - modified InnoDB transaction code for snapshot selection
* mysql/sql/gtt.{h,cc} - core Skeena implementation

### Build ERMIA:
* Follow sample instructions in ermia/README.md

### Build MySQL:
* Follow sample instructions under mysql/ermia-contrib/BUILD

### Build and Run Postgres (WIP):
1. Build ERMIA first.
2. Build Postgres.
```
$ cd postgres
$ mkdir build
$ cd build
$ ../configure --prefix=/home/$USER/path/to/postgres --enable-ermia=yes --with-ermia=/home/$USER/path/to/skeena/ermia
$ make
$ make install
```
3. Run Postgres server.
```
$ rm -rf /temp/path/to/postgres/data/*
$ rm -rf /temp/path/to/ermia-log/*
$ /home/$USER/path/to/postgres/bin/initdb -D /tmp/path/to/postgres/data
$ LD_PRELOAD=/home/$USER/path/to/skeena/ermia/build/lib/libermia_api.so /home/$USER/path/to/postgres/bin/postgres -D /tmp/path/to/postgres/data
```
4. Run Postgres client.
```
$ /home/$USER/path/to/postgres/bin/createdb
$ /home/$USER/path/to/postgres/bin/psql
```

### Environment:
* MySQL settings: follow the mysql/ermia-contrib/my.cnf file and put it in your ~/.my.cnf
* Useful variables in my.cnf:
  - ermia-ermia-log-dir: Path to the ERMIA engine log dir
  - ermia-gtt-recycle-capacity: The size of each GTT(CSR) Tree
  - ermia-gtt-recycle-threshold: The threshold to trigger the recycle
* To start MySQL Server: `./build/bin/mysqld`

### Experiments:
* All scripts are stored under mysql/ermia-contrib
  - Microbenchmark: see the mysql/ermia-contrib/skeena-benchrunner folder
  - TPC-C: see ermia-contrib/tpcc/README and check the scripts there
