#! /usr/bin/env python
# coding=gbk
import logging, os


class Logger:
    def __init__(self, path, slevel=logging.DEBUG, flevel=logging.DEBUG):
        self.logger = logging.getLogger(path)
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
        # LOG on screen
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(slevel)
        # LOG in file
        fh = logging.FileHandler(path)
        fh.setFormatter(fmt)
        fh.setLevel(flevel)
        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def war(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)

    def cri(self, message):
        self.logger.critical(message)


if __name__ == '__main__':
    logyyx = Logger("C:\\Users\\wangrongkai\\Desktop\\123\\example.log", logging.INFO, logging.DEBUG)
    logyyx.debug('һ��debug��Ϣ')
    logyyx.info('һ��info��Ϣ')
    logyyx.war('һ��warning��Ϣ')
    logyyx.error('һ��error��Ϣ')
    logyyx.cri('һ������critical��Ϣ')