#!/bin/env/python3
import mysql.connector
import sys
import time


# TODO: Create the TABLE with mysql connector

init_stmt = (
        """
DROP DATABASE IF EXISTS demo;
CREATE DATABASE IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.inno (`key` BIGINT(20), `value` VARCHAR(20));
CREATE TABLE IF NOT EXISTS demo.orz (
  id BIGINT NOT NULL,
  value CHAR(10) NOT NULL,
  PRIMARY KEY (id)
  -- INDEX idx_v(value)
) ENGINE = ERMIA;
        """
        )
touch_ermia = ("SELECT * FROM demo.orz")
touch_inno = ("SELECT * FROM demo.inno")
insert_inno = ("INSERT INTO demo.inno(`key`, value) VALUES(900, \"foo\");")

try:
    conn_1 = mysql.connector.connect(unix_socket="/tmp/mysql.sock", user="root")
    conn_2 = mysql.connector.connect(unix_socket="/tmp/mysql.sock", user="root", autocommit=True)
except mysql.connector.Error as err:
    print(err)
    sys.exit(-1)

# T0 starts first
cur_1 = conn_1.cursor()
try:
    cur_1.execute(init_stmt, multi=True)
except mysql.connector.Error as err:
    print("Init error {}".format(err))
    sys.exit(-1)
print(cur_1.fetchwarnings())
# T0 BEGIN;
cur_1.execute(touch_ermia)
cur_1.fetchall()

# T1 BEGIN AUTOCOMMIT;
cur_2 = conn_2.cursor()
# ADVANCE ERMIA TIMESTAMP
cur_2.execute(touch_ermia)
cur_2.fetchall()
conn_2.commit()
# BLIND WRITE
cur_2.execute(insert_inno)
conn_2.commit()
# FETCH READVIEW
cur_2.execute(touch_inno)
cur_2.fetchall()
conn_2.commit()
# BLIND WRITE AGAIN
cur_2.execute(insert_inno)
conn_2.commit()

# FETCH READVIEW AGAIN
cur_2.execute(touch_inno)
cur_2.fetchall()
conn_2.commit()

# T1 FINISH
cur_2.close()

# T0 FETCH READVIEW -- It should only see one row of record.
cur_1.execute(touch_inno)
result = cur_1.fetchall()
for (out_key, out_value) in result:
    print("[{}] {}".format(out_key, out_value))

print("record number = {}".format(len(result)))
cur_1.close()
conn_1.close()
conn_2.close()
