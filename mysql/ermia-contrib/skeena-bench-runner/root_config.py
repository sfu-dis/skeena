import toml
import os
import sys
import logging

logging.basicConfig()

class RootConfig:
    def __init__(self, file_name: str):
        log = logging.getLogger(__name__)
        try:
            with open(file_name, "r") as f:
                self._data = toml.load(f)
                log.info(self)
        except Exception as e:
            log.error("Fail to load the toml file {}".format(e))
            sys.exit(-1)
