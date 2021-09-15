#!/bin/env/python3
import mysql.connector
import sys
import time


# TODO: Create the TABLE with mysql connector

init_stmt = """
CREATE DATABASE IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.inno (`key` BIGINT(20), `value` VARCHAR(20));
CREATE TABLE IF NOT EXISTS demo.inno2 (`key` BIGINT(20), `value` VARCHAR(20));
CREATE TABLE IF NOT EXISTS demo.orz (
  id BIGINT NOT NULL,
  value CHAR(10) NOT NULL
) ENGINE = ERMIA;
        """
touch_ermia = ("SELECT * FROM demo.orz")
touch_inno = ("SELECT * FROM demo.inno")
insert_ermia = ("INSERT INTO demo.orz VALUE(1, \"qwq\")")
insert_inno = ("INSERT INTO demo.inno(`key`, value) VALUES(900, \"foo\")")
update_inno = ("UPDATE demo.inno SET value = \"script-thd4\" WHERE `key` = 900")
insert_inno2 = ("INSERT INTO demo.inno2(`key`, value) VALUES(900, \"foo\")")


try:
    conn_1 = mysql.connector.connect(unix_socket="/tmp/mysql.sock", user="root")
    conn_2 = mysql.connector.connect(unix_socket="/tmp/mysql.sock", user="root")
except mysql.connector.Error as err:
    print(err)
    sys.exit(-1)

print("{}".format(conn_1.autocommit))
print("{}".format(conn_2.autocommit))
# T0 starts first
cur_1 = conn_1.cursor()
try:
    cur_1.execute(init_stmt, multi=True)
    conn_1.commit()
except mysql.connector.Error as err:
    print("Init error {}".format(err))
    sys.exit(-1)

print(cur_1.fetchwarnings())

# T0 BEGIN;
cur_1.execute("BEGIN")
cur_1.execute(touch_ermia)
cur_1.fetchall()

cur_1.execute(touch_inno)
cur_1.fetchall()


# T1 BEGIN ;
cur_2 = conn_2.cursor()
# T1: ADVANCE ERMIA TIMESTAMP
cur_2.execute(touch_ermia)
cur_2.fetchall()
conn_2.commit()

# T1: BLIND WRITE
cur_2.execute(insert_inno)
conn_2.commit()

# T1: TOUCH ERMIA
cur_2.execute("BEGIN")
cur_2.execute(touch_ermia)
cur_2.fetchall()

cur_2.execute(touch_inno)
cur_2.fetchall()

# FETCH READVIEW
print("PERFORMING MODIFY ...")
cur_2.execute(update_inno)  # Get trx_id = 1000 This will rollback
cur_2.execute(insert_ermia) # This will rollback
print(cur_2.fetchwarnings())
cur_1.execute(insert_inno2) # Get trx_id = 1001
print(cur_1.fetchwarnings())

conn_1.commit()
print(cur_1.fetchwarnings())
time.sleep(3)
try:
    conn_2.commit()
except mysql.connector.Error as err:
    print(err)
print(cur_2.fetchwarnings())
time.sleep(3)
# BLIND WRITE AGAIN

cur_1.close()
cur_2.close()
# FETCH READVIEW AGAIN

conn_1.close()
conn_2.close()
print("DONE")
