# -*- coding: utf-8 -*-
import imaplib
import email
import email.message
import mimetypes
import logging
import logging.config
import traceback
import datetime
import os
import sqlite3
import argparse
from configparser import ConfigParser

CONFIG_FILE = "config.ini"
DEFAULT_DATE_FORMAT = "%d.%m.%Y"
DATE_FORMAT_FOR_IMAP = "%d-%b-%Y"
LOG_CONF_FILE_NAME = "logging.conf"


class ParamNotSet:
    pass


class EmailDownloader:
    """
        Класс для скачивания писем и вложений к ним
    """

    def __init__(self):
        pass

    def connect_mailbox(self, login: str, password: str, mailbox="inbox"):
        pass

    def download_msg(self, uid, is_download_msg=True, is_download_payload=True):
        pass

    def download_all_msg(self, count=ParamNotSet()):
        pass

    def save_msg(self, p_msg):
        pass

    def save_msg_payload(self, p_msg):
        pass

    def set_downloaded_msg_path(self, p_path_name):
        pass

    def set_downloaded_payload_path(self, p_path_name):
        pass


if __name__ == "__main__":
    if not os.path.exists(LOG_CONF_FILE_NAME):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger_handler = logging.FileHandler('email_loader.log')
        logger_handler.setLevel(logging.DEBUG)
        logger_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        logger_handler.setFormatter(logger_formatter)
        logger.addHandler(logger_handler)
    else:
        logging.config.fileConfig(LOG_CONF_FILE_NAME)
        logger = logging.getLogger()
