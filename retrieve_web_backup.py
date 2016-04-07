#!/usr/bin/env python

import sys
import logging
import re
import boto3
import datetime
import os
from util import util


# Fake class only for purpose of limiting global namespace to the 'g' object
class g:
    program_filename = None 


def main(argv):

    g.program_filename = os.path.basename(__file__)
    if g.program_filename[-3:] == '.py':
        g.program_filename = g.program_filename[:-3]

    # Load AWS creds which are used for iterating S3 backups and creating download link
    aws_access_key_id = util.get_ini_setting('aws', 'access_key_id', False)
    aws_secret_access_key = util.get_ini_setting('aws', 'secret_access_key', False)
    aws_region_name = util.get_ini_setting('aws', 'region_name', False)
    aws_s3_bucket_name = util.get_ini_setting('aws', 's3_bucket_name', False)

    # Find latest backup in 'daily' folder of S3 bucket 'ingomarchurch_website_backups'
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name)
    file_items = [item for item in s3.Bucket(aws_s3_bucket_name).objects.all() if item.key[-1] != '/']
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
                    message("Unrecognized file in 'daily' backup folder...ignoring: " + file_item.key)
        else:
            message('Unrecognized folder or file in website_backups S3 bucket with long path...ignoring: ' +
                file_item.key)
    if obj_to_retrieve is not None:
        # Generate 10-minute download URL
        s3Client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name)
        url = s3Client.generate_presigned_url('get_object', Params = {'Bucket': aws_s3_bucket_name,
            'Key': obj_to_retrieve.key}, ExpiresIn = 10 * 60)
        print url
    else:
        message('Error finding latest backup file to retrieve. Aborting!')
        sys.exit(1)

    sys.exit(0)


def message(str):
    global g

    datetime_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print >> sys.stderr, datetime_stamp + ':' + g.program_filename + ':' + level + ':' + s


if __name__ == "__main__":
    main(sys.argv[1:])
