## Skeena: Efficient and Consistent Cross-Engine Transactions

Skeena enables consistent ACID cross-engine transactions over two database engines (e.g., InnoDB and ERMIA, both in MySQL) without intrusive changes to individual engines. 

This repository implements Skeena and integrates the [ERMIA](https://github.com/sfu-dis/ermia) (SIGMOD 2016) main-memory database engine into MySQL alongside with its default InnoDB engine.

See details in our SIGMOD 2022 paper below. If you use our work, please cite:

```
Skeena: Efficient and Consistent Cross-Engine Transactions.
Jianqiu Zhang, Kaisong Huang, Tianzheng Wang and King Lv.
ACM SIGMOD/PODS International Conference on Management of Data 2022
```

### Directory structure and notable source files:
* mysql/ - modified MySQL to support ERMIA and Skeena
* mysql/innobase/trx/trx0trx.cc - modified InnoDB transaction code for snapshot selection
* mysql/sql/gtt.{h,cc} - core Skeena implementation

### Build ERMIA:
* Follow sample instructions in ermia/README.md

### Build MySQL:
* Follow sample instructions under mysql/ermia-contrib/BUILD

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
