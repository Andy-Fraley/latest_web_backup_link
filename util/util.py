#!/usr/bin/env python

import os
import sys
import ConfigParser
import logging


def get_ini_setting(section, option, none_allowable=True):
    config_file_path = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + '/../retrieve_web_backup.ini')
    if not os.path.isfile(config_file_path):
        logging.error("Required ini file '" + config_file_path +
            "' is missing. Clone file 'retrieve_web_backup__sample.ini' to create file 'retrieve_web_backup.ini'")
        sys.exit(1)
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config_file_path)
    try:
        ret_val = config_parser.get(section, option).strip()
    except:
        ret_val = None
    if ret_val == '':
        ret_val = None
    if not none_allowable and ret_val == None:
        logging.error("Required setting in retrieve_web_backup.ini '[" + section + ']' + option +
            "' cannot be missing or blank")
        sys.exit(1)
    return ret_val
