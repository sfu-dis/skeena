import os
import requests
import time
import subprocess
import sys
from datetime import datetime

class RunConfig:
    def __init__(self, name: str):
        self.txns = [-1, 10, 20, 21, 22, 23]
        self.mappings = ["IIIIIIIII", "IIEIIIIII", "IIEIIIIIE", "EIEIIIIIE", "EEEIIIIIE", "EEEEIIIIE", "EEEEEIIIE", "EEEEEEIIE", "EEEEEEEIE", "EEEEEEEEE"]
        self.thds = [1, 10, 20, 30, 40, 50]
        self.name = name
        self.CSR = True

    def setup(self, txns, mappings, thds):
        self.txns = txns
        self.mappings = mappings
        self.thds = thds




def notify_me(name, thd, mapping, txn, err, ok = True):
    TGAPI = "https://api.telegram.org/bot367237569:AAFkTrJo5Ijz2JRJOTz0iRkuMabN_0plPF4/sendMessage"
    payload = {
        "text": "",
        "parse_mode": "markdown",
        "chat_id": 212164543
    }
    if not ok:
        payload["text"] = "[FAIL] Experiment name: {}, threads = {}, engine-scheme = {}, txn = {}, err = `{}`".format(name, thd, mapping, txn, err)
    else:
        payload["text"] = "`[PASS] Experiment name: {}, threads = {}, engine-scheme = {}, txn = {}`".format(name, thd, mapping, txn)
    try:
        requests.get(TGAPI, params=payload)
    except Exception as e:
        pass

def run(engine, txn, thd, CSR, dirname):
    sleep_time = 20
    try:
        init = False
        while not init:
            os.system("pgrep mysqld | xargs kill")
            os.system("rm -rf /dev/shm/void001/mysql/*")
            os.system("/home/void001/skeena/build/bin/mysqld --initialize")
            if CSR:
                os.system("./start_mysqld.sh &")
            else:
                os.system("./start_mysqld.sh --ermia-ermia-bypass=TRUE &")
            time.sleep(sleep_time)
            ret = 0

            # For large bpool, use 50 40
            # For small bpool, use 200 40 here
            # Change both lines

            if engine[5] == "E":
                ret = subprocess.call("./tpcc-prepare.sh 50 40 {}".format(engine), shell = True)
            else:
                ret = subprocess.call("./inno-tpcc-prepare.sh 50 40 {}".format(engine), shell = True)
            if ret != 0:
                print("!!!! WARNING !!!! Initialization Failed, restart")
            else:
                init = True
        if engine[5] == "E":
            ret = subprocess.call("./run.sh {} {} {} {}".format(thd, txn, engine, dirname), shell=True)
        else:
            ret = subprocess.call("./run-inno.sh {} {} {} {}".format(thd, txn, engine, dirname), shell=True)
        if ret != 0:
            notify_me(dirname, thd, engine, txn, "Benchmark run failed", False)
        else:
            notify_me(dirname, thd, engine, txn, "")
        os.system("pgrep mysqld | xargs kill")
    except Exception as e:
        notify_me(dirname, thd, engine, txn, e, False)



def run_helper(conf: RunConfig):
    dirname = conf.name
    os.system("mv {} {}-{}".format(dirname, dirname, datetime.now().strftime("%Y-%m-%d-%H:%M")))
    os.system("mkdir {}".format(dirname))

    # Here, configure the experiments you want to run
    thds = conf.thds

    # -1: Mix, 10: New-Order, 20: Payment,
    # 21: Order-Status, 22: Delivery, 23: Stock-Level
    txns = conf.txns

    # For mappings, check README.md for the order
    mappings = conf.mappings

    for engine in mappings:
        for thd in thds:
            for txn in txns:
                run(engine, txn, thd, conf.CSR, dirname)

if __name__ == "__main__":
    configs = []
    heatmap_txns = RunConfig("heatmap-txns-large")
    heatmap_thds = RunConfig("heatmap-thds-large")
    heatmap_gap = RunConfig("heatmap-gap-large")
    scattered = RunConfig("scattered-large")
    csr_on_overhead = RunConfig("csr-on-overhead-large")
    csr_off_overhead = RunConfig("csr-off-overhead-large")

    # TEMPORARY
    # rerun_heatmap_txns = RunConfig("re-heatmap-txns-small")
    # rerun_heatmap_gap = RunConfig("re-heatmap-gap-small")

    txns = [-1, 10, 20, 21, 22, 23]
    thds = [50]
    mappings = ["IIIIIIIII", "IIEIIIIII", "IIEIIIIIE", "EIEIIIIIE", "EEEIIIIIE"
                , "EEEEIIIIE", "EEEEEIIIE", "EEEEEEIIE", "EEEEEEEIE", "EEEEEEEEE"]
    heatmap_txns.setup(txns, mappings, thds)
    configs.append(heatmap_txns)

    mappings = ["IIIIIIIII", "IIEIIIIII", "IIEIIIIIE", "EIEIIIIIE", "EEEIIIIIE", "EEEEIIIIE", "EEEEEIIIE", "EEEEEEIIE", "EEEEEEEIE", "EEEEEEEEE"]
    txns = [-1]
    thds = [1, 10, 20, 30, 40, 50]
    heatmap_thds.setup(txns, mappings, thds)
    configs.append(heatmap_thds)

    mappings = ["EEEEEIIIE", "EEEEEEIIE", "IIIIIIIII", "IIIIIEIII"]
    txns = [10, 20, 21, 22, 23]
    thds = [1, 10, 20, 30, 40, 50]
    heatmap_gap.setup(txns, mappings, thds)
    configs.append(heatmap_gap)

    mappings = ["EEEEEEEEE", "IIIIIIIII", "IIEIIIIII", "IIEIIIIIE", "EEEIEEEEE"]
    txns = [-1, 10, 20, 21, 22, 23]
    thds = [1, 10, 20, 30, 40, 50]
    scattered.setup(txns, mappings, thds)
    configs.append(scattered)

    mappings = ["IIIIIIIII", "EEEEEEEEE"]
    txns = [-1]
    thds = [1, 10, 20, 30, 40, 50]
    csr_on_overhead.setup(txns, mappings, thds)
    csr_off_overhead.setup(txns, mappings, thds)
    csr_off_overhead.CSR = False
    configs.append(csr_on_overhead)
    configs.append(csr_off_overhead)

    # mappings = ["EEEIIIIIE", "EEEEIIIIE", "EEEEEEIIE", "EEEEEEEIE"]
    # thds = [50]
    # txns = [10]
    # rerun_heatmap_txns.setup(txns, mappings, thds)
    # configs.append(rerun_heatmap_txns)

    # mappings = ["EEEEEIIIE"]
    # thds = [40, 50]
    # txns = [10]
    # rerun_heatmap_gap.setup(txns, mappings, thds)
    # configs.append(rerun_heatmap_gap)

    for config in configs:
        print("RunConfig to run: {}".format(config.name))

    for config in configs:
        run_helper(config)

    notify_me("ALL", 9999, "ALL", 9999, "")

