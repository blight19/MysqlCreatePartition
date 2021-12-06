#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:yanshushuang
# datetime:2021/10/25

import logging
import os.path
import time


class Logger:
    def __init__(self, stream=False, file=False, log_path='./'):
        self.logger = logging.getLogger("Partition Logger")
        self.formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
        self.logger.setLevel(logging.INFO)
        if stream:
            self.__add_stream()
        if file:
            self.log_path = log_path
            self.__add_file()


    def __add_stream(self):
        sh = logging.StreamHandler()
        sh.setFormatter(self.formatter)
        self.logger.addHandler(sh)

    def __add_file(self):
        filename = time.strftime("%Y-%m-%d")
        fh = logging.FileHandler(filename=os.path.join(self.log_path,filename))
        fh.setFormatter(self.formatter)
        self.logger.addHandler(fh)

    def get_logger(self):
        return self.logger
