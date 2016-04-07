#!/usr/bin/env python

import sys
import datetime
import logging
import argparse
import os
import shutil
import tempfile
import subprocess
import ConfigParser
import re
import calendar
import boto3
from util import util
import pytz

# Fake class only for purpose of limiting global namespace to the 'g' object
class g:
    args = None
    output_directory = None
    message_output_filename = None
    aws_access_key_id = None
    aws_secret_access_key = None
    aws_region_name = None
    aws_s3_bucket_name = None


def main(argv):
    global g

    parser = argparse.ArgumentParser()
    parser.add_argument('--output-directory', required=False, help='Output directory where web files are unpacked. ' +
        'Typically /var/www/html is specified, but defaults to current working directory.')
    parser.add_argument('--message-output-filename', required=False, help='Filename of message output file. If ' +
        'unspecified, then messages are written to stderr as well as into the messages_[datetime_stamp].log file ' +
        'that is zipped into the resulting backup file.')

    g.args = parser.parse_args()

    g.program_filename = os.path.basename(__file__)
    if g.program_filename[-3:] == '.py':
        g.program_filename = g.program_filename[:-3]

    message_level = util.get_ini_setting('logging', 'level')

    g.temp_directory = tempfile.mkdtemp(prefix='retrieve_web_backup_')

    g.message_output_filename = g.temp_directory + '/messages_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + \
        '.log'

    util.set_logger(message_level, g.message_output_filename, os.path.basename(__file__))

    # Load AWS creds which are used for checking need for backup and posting backup file
    g.aws_access_key_id = util.get_ini_setting('aws', 'access_key_id', False)
    g.aws_secret_access_key = util.get_ini_setting('aws', 'secret_access_key', False)
    g.aws_region_name = util.get_ini_setting('aws', 'region_name', False)
    g.aws_s3_bucket_name = util.get_ini_setting('aws', 's3_bucket_name', False)

    obj_to_retrieve = get_latest_wp_backup()
    if obj_to_retrieve is not None:
        backup_file_path = g.temp_directory + '/' + obj_to_retrieve.key.split('/')[1]
        message_info('Retrieving ' + backup_file_path + '. NOTE - Backup files are *large* and download from S3 ' + \
            'can take minutes.')
        obj_to_retrieve.download_file(backup_file_path)
        message_info(backup_file_path + ' downloaded.')
    else:
        message_error('Error finding latest backup file to retrieve. Aborting!')
        util.sys_exit(1)

    util.sys_exit(0)


def upload_to_s3(folder_name, output_filename):
    global g

    # Cache and reuse exact same S3 filename even if upload_to_s3 called multiple times for daily, weekly, etc.
    if g.reuse_output_filename is None:
        g.reuse_output_filename = datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '.zip'

    s3_key = folder_name + '/' + g.reuse_output_filename
    s3 = boto3.resource('s3', aws_access_key_id=g.aws_access_key_id, aws_secret_access_key=g.aws_secret_access_key,
        region_name=g.aws_region_name)
    data = open(output_filename, 'rb')
    bucket = s3.Bucket(g.aws_s3_bucket_name)
    bucket.put_object(Key=s3_key, Body=data)
    message_info('Uploaded to S3: ' + s3_key)
    return s3_key


def download_from_s3(obj):
    obj.download_file('tmp.bin')


def gen_s3_expiring_url(s3_key, expiry_days):
    global g

    s3Client = boto3.client('s3', aws_access_key_id=g.aws_access_key_id, aws_secret_access_key=g.aws_secret_access_key,
        region_name=g.aws_region_name)
    url = s3Client.generate_presigned_url('get_object', Params = {'Bucket': g.aws_s3_bucket_name, 'Key': s3_key},
        ExpiresIn = expiry_days * 24 * 60 * 60)
    return url


def delete_from_s3(item_to_delete):
    item_to_delete_key = item_to_delete.key
    item_to_delete.delete()
    message_info('Deleted from S3: ' + item_to_delete_key)


def send_email_notification(list_completed_backups, list_notification_emails):
    global g

    body = ''
    sep = ''
    backup_completed_str = 'Backup(s) completed'
    for completed_backup in list_completed_backups:
        folder_name = completed_backup[0]
        url = completed_backup[1]
        expiry_days = completed_backup[2]
        body = body + sep + 'Completed ' + folder_name + ' backup which is accessible at ' + url + ' for ' + \
            str(expiry_days) + ' days.'
        sep = '\r\n\r\n'
    if g.run_util_errors is not None and len(g.run_util_errors) > 0:
        body = body + sep + 'There were errors running the following utility(s): ' + ', '.join(g.run_util_errors) + \
            '. See messages_xxx.log in backup zip file for details.'
        backup_completed_str = backup_completed_str + ' with errors'
    util.send_email(list_notification_emails, backup_completed_str, body)


def get_latest_wp_backup():
    global g

    s3 = boto3.resource('s3', aws_access_key_id=g.aws_access_key_id, aws_secret_access_key=g.aws_secret_access_key,
        region_name=g.aws_region_name)

    # In S3, folder items end with '/', whereas files do not
    file_items = [item for item in s3.Bucket(g.aws_s3_bucket_name).objects.all() if item.key[-1] != '/']
    files_per_folder_dict = {}
    newest_sortable_str = ''
    obj_to_retrieve = None
    for file_item in file_items:
        path_sects = file_item.key.split('/')
        if len(path_sects) == 2:
            if path_sects[0] == 'daily':
                filename = path_sects[1]
                match = re.match('backwpup_[0-9a-f]{6}_(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})' + \
                    '_(?P<hours>[0-9]{2})-(?P<minutes>[0-9]{2})-(?P<seconds>[0-9]{2})\.tar\.gz', filename)
                if match is not None:
                    sortable_str = match.group('year') + match.group('month') + match.group('day') + \
                        match.group('hours') + match.group('minutes') + match.group('seconds')
                    if sortable_str > newest_sortable_str:
                        newest_sortable_str = sortable_str
                        obj_to_retrieve = file_item
                else:
                    message_info("Unrecognized file in 'daily' backup folder...ignoring: " + file_item.key)
        else:
            message_info('Unrecognized folder or file in website_backups S3 bucket with long path...ignoring: ' +
                file_item.key)
    return s3.Bucket(g.aws_s3_bucket_name).Object(obj_to_retrieve.key)


def get_schedules_from_ini():
    config_file_path = os.path.dirname(os.path.abspath(__file__)) + '/ccb_backup.ini'
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config_file_path)
    schedules = []
    curr_datetime = datetime.datetime.now(pytz.UTC)
    message_info('Current UTC datetime: ' + str(curr_datetime))
    for schedule in config_parser.items('schedules'):
        schedule_parms = schedule[1].split(',')
        if len(schedule_parms) != 3:
            message_error("ccb_backup.ini [schedules] entry '" + schedule[0] + '=' + schedule[1] + "' is invalid. " \
                "Must contain 3 comma-separated fields. Aborting!")
            util.sys_exit(1)
        folder_name = schedule_parms[0].strip()
        delta_time_string = schedule_parms[1].strip()
        num_files_to_keep_string = schedule_parms[2].strip()
        try:
            num_files_to_keep = int(num_files_to_keep_string)
        except:
            message_error("ccb_backup.ini [schedules] entry '" + schedule[0] + '=' + schedule[1] + "' is " \
                "invalid. '" + num_files_to_keep_string + "' must be a positive integer")
            util.sys_exit(1)
        if num_files_to_keep < 0:
                message_error("ccb_backup.ini [schedules] entry '" + schedule[0] + '=' + schedule[1] + "' is " \
                "invalid. Specified a negative number of files to keep")
                util.sys_exit(1)
        backup_after_datetime = now_minus_delta_time(delta_time_string)
        if backup_after_datetime is None:
            message_error("ccb_backup.ini [schedules] entry '" + schedule[0] + '=' + schedule[1] + "' contains " \
                "an invalid interval between backups '" + delta_time_string + "'. Aborting!")
            util.sys_exit(1)
        schedules.append({'folder_name': folder_name, 'backup_after_datetime': backup_after_datetime,
            'num_files_to_keep': num_files_to_keep})
    return schedules


def now_minus_delta_time(delta_time_string):
    curr_datetime = datetime.datetime.now(pytz.UTC)
    slop = 15 * 60 # 15 minutes of "slop" allowed in determining new backup is needed
    # curr_datetime = datetime.datetime(2016, 1, 7, 10, 52, 23, tzinfo=pytz.UTC)
    match = re.match('([1-9][0-9]*)([smhdwMY])', delta_time_string)
    if match is None:
        return None
    num_units = int(match.group(1))
    unit_char = match.group(2)
    seconds_per_unit = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}
    if unit_char in seconds_per_unit:
        delta_secs = (int(seconds_per_unit[unit_char]) * num_units) - slop
        return curr_datetime - datetime.timedelta(seconds=delta_secs)
    elif unit_char == 'M':
        month = curr_datetime.month - 1 - num_units
        year = int(curr_datetime.year + month / 12)
        month = month % 12 + 1
        day = min(curr_datetime.day, calendar.monthrange(year, month)[1])
        return datetime.datetime(year, month, day, curr_datetime.hour, curr_datetime.minute, curr_datetime.second,
            tzinfo=pytz.UTC) - datetime.timedelta(seconds=slop)
    else: # unit_char == 'Y'
        return datetime.datetime(curr_datetime.year + num_units, curr_datetime.month, curr_datetime.day,
            curr_datetime.hour, curr_datetime.minute, curr_datetime.second, tzinfo=pytz.UTC) - \
            datetime.timedelta(seconds=slop)


def run_util(util_name, second_util_name=None):
    global g

    if util_name == 'attendance' and g.args.all_time:
        all_time_list = [ '--all-time' ]
    else:
        all_time_list = []

    datetime_stamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    util_py = 'get_' + util_name + '.py'
    fullpath_util_py = os.path.dirname(os.path.realpath(__file__)) + '/' + util_py
    if second_util_name is not None:
        output_filename1 = g.temp_directory + '/' + util_name + '_' + datetime_stamp + '.csv'
        output_filename2 = g.temp_directory + '/' + second_util_name + '_' + datetime_stamp + '.csv'
        outputs_list = ['--output-' + util_name + '-filename', output_filename1,
            '--output-' + second_util_name + '-filename', output_filename2]
        message_info('Running ' + util_py + ' with output files ' + output_filename1 + ' and ' + \
            output_filename2)
    else:
        output_filename = g.temp_directory + '/' + util_name + '_' + datetime_stamp + '.csv'
        outputs_list = ['--output-filename', output_filename]
        message_info('Running ' + util_py + ' with output file ' + output_filename)
    exec_list = [fullpath_util_py] + all_time_list + ['--message-output-filename', g.message_output_filename] + \
        outputs_list
    exit_status = subprocess.call(exec_list)
    if exit_status == 0:
        message_info('Successfully ran ' + util_py)
    else:
        message_warning('Error running ' + util_py + '. Exit status ' + str(exit_status))
        g.run_util_errors.append(util_py)


def message_info(s):
    logging.info(s)
    output_message(s, 'INFO')


def message_warning(s):
    logging.warning(s)
    output_message(s, 'WARNING')


def message_error(s):
    logging.error(s)
    output_message(s, 'ERROR')


def output_message(s, level):
    global g

    if g.args.message_output_filename is None:
        datetime_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print >> sys.stderr, datetime_stamp + ':' + g.program_filename + ':' + level + ':' + s


if __name__ == "__main__":
    main(sys.argv[1:])
