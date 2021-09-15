from root_config import RootConfig
import argparse
import time
import os
import inotify.adapters
import subprocess
import logging
import shlex
import json
import sys

from pprint import pprint

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
CREATE_THDS = 64


# Wait for one file to be exist (create), no recursive watching
def wait_for_file(path: str):
    if os.path.isdir(path):
        raise Exception("Path should refer to file, not dir")
    if os.path.exists(path):
        return
    dname = os.path.dirname(path)
    fname = os.path.basename(path)
    h = inotify.adapters.Inotify()
    h.add_watch(dname)

    for ev in h.event_gen(timeout_s = 180, yield_nones = False):
        (_, ev_type, ev_path, ev_name) = ev
        logger.info("Event: {}, File: {}".format(ev_type, ev_name))
        if ev_type[0] == "IN_CREATE" and ev_name == fname:
            logger.info("File {} is created".format(path))
            return

    raise Exception("File didn't appear after 180s, exit now")


class BenchRunner:
    def __init__(self, rc: RootConfig):
        self.BASE_COMMAND = """
taskset -c {} sysbench {} --mysql-ignore-errors={} --mysql-socket={} \
--mysql_storage_engine=ERMIA \
--create-secondary=off \
--db-driver=mysql \
--range_selects=off \
"""
        # logger.info("{}".format(json.dumps(cfg))
        cfg = rc._data
        self.cpuset = cfg["system"]["cpuset"]
        self.ignore_errors = cfg["system"]["ignore_errors"]
        self.socket = cfg["system"]["socket"]
        self.bench = cfg["benchmark"]
        self.cwd = cfg["system"].get("cwd", os.getcwd())
        self.log_dir = cfg["system"].get("log_dir", "")
        os.system("cp prepare_tables.sh start_mysqld.sh {}".format(self.cwd))
        # Generate base command

    def gen_command(self, item, conn, inno_percentage) -> str:
        mapping_tbl = {
            "random_type": "--rand-type",
            "table_count": "--tables",
            "table_size": "--table_size",
            "duration": "--time",
            "interval": "--report-interval",
            "db": "--mysql-db",
            "read": "--point_selects",
            "write": "--non_index_updates",
            # "innodb_percentage": "--innodb_percentage",
        }
        cmd = self.BASE_COMMAND.format(
            self.cpuset, item["bench_script"], ",".join(self.ignore_errors),
            self.socket)


        for argname, argval in item.items():
            if argname in mapping_tbl:
                cmd = cmd + " {}={}".format(mapping_tbl[argname], argval)

        cmd = cmd + " --innodb_percentage={} --threads={}".format(inno_percentage, conn)
        cmd = cmd + " run"
        return cmd


    def run(self):
        ts = time.time()
        os.chdir(os.path.join(self.cwd))
        for item in self.bench:
            for conn in item["connection"]:
                for percentage in item.get("innodb_percentage", [0]):
                    cmd = self.gen_command(item, conn, percentage)
                    run_id = "{}-conn_{}-inno_{}@{}".format(item["id"], conn, percentage, ts)
                    logger.info("Dry run for id {}: \033[1;32m{}\033[0m".format(run_id, cmd))
                    run_cmd = shlex.split(cmd)
                    self.prepare(item)
                    self.exec(run_cmd, run_id, item.get("nround", 1))

    def prepare(self, item):
        logger.info("(Re)starting mysqld")
        _tmp = os.system("pgrep -u $USER mysqld | xargs kill") #.format(item.get("table_count"), item.get("table_size"), CREATE_THDS))
        _tmp = os.system("sleep 30s") #.format(item.get("table_count"), item.get("table_size"), CREATE_THDS))
        _tmp = os.system("./start_mysqld.sh &") #.format(item.get("table_count"), item.get("table_size"), CREATE_THDS))
        _tmp = os.system("sleep 30s")
        logger.info("Wait & Prepare tables for benchmark")
        wait_for_file(self.socket)
        _tmp = os.system("./prepare_tables.sh {} {} {} >/dev/null".format(item.get("table_count"), item.get("table_size"), CREATE_THDS))
        logger.info("Tables are ready")


    def exec(self, run_cmd, run_id, cnt=1):
        logger.info("Executing benchmark id = {}".format(run_id))
        try:
            with open(os.path.join("logs", self.log_dir, "{}.log").format(run_id), "w") as f:
                f.write("Command: {}\n".format(" ".join(run_cmd)))
                for epoch in range(1, cnt + 1):
                    f.write("= RUN {} =\n".format(epoch))
                    result = subprocess.run(run_cmd, capture_output=True, check=True, text=True)
                    f.write(result.stdout)
        except subprocess.CalledProcessError as cpe:
            logger.error("Benchmark exit with non-zero code: {}.\n{}".format(cpe.returncode, cpe.output))
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skeena benchmark runner")
    parser.add_argument("--config", dest="config_name",
            help="Specify the config file path", required=True)
    args = parser.parse_args()

    root = RootConfig(file_name=args.config_name)
    runner = BenchRunner(root)
    runner.run()
