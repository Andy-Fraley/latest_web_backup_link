#!/usr/bin/env python

import os
import sys
import ConfigParser
import logging


def get_ini_setting(section, option, none_allowable=True):
    config_file_path = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + '/../latest_web_backup_link.ini')
    if not os.path.isfile(config_file_path):
        logging.error("Required ini file '" + config_file_path +
            "' is missing. Clone file 'latest_web_backup_link__sample.ini' to create file " + \
            "'latest_web_backup_link.ini'")
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
        logging.error("Required setting in latest_web_backup_link.ini '[" + section + ']' + option +
            "' cannot be missing or blank")
        sys.exit(1)
    return ret_val
