#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Using boto sync files to s3. Creates a file of MD5 to compare for future
syncs.

"""
import sys
import os
import boto
import hashlib
import json
from boto.s3.key import Key

S3_STATIC_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY') 

class StaticSync:
  def __init__(self, sync_dir):
    self.sync_dir = sync_dir
    bucket_name = S3_STATIC_BUCKET_NAME
    conn = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    self.bucket = conn.get_bucket(bucket_name)

  def get_md5s(self):
    # Get a list of all current files
    all_files = {}
    for dirname, dirnames, filenames in os.walk(self.sync_dir):
      for filename in filenames:
        path = os.path.join(dirname, filename)
        md5 = hashlib.md5(open(path, 'r').read()).hexdigest()
        all_files[path] = md5
    return all_files
  
  def sync_s3_files(self, changed_files=[], remove_files=[]):
    length = len(changed_files)
    for idx, path in enumerate(changed_files):
      name = path.replace(self.sync_dir + '/', '')
      print 'Sending...', idx + 1, ' of ', length, ' ', path, ' to ', name

      aws_key = Key(self.bucket)
      aws_key.key = name 
      aws_key.set_contents_from_filename(path)

    length = len(remove_files)
    for idx, path in enumerate(remove_files):
      name = path.replace(self.sync_dir + '/', '')
      print 'Removing...', idx + 1, ' of ', length, ' ', name
      aws_key = Key(self.bucket)
      aws_key.key = name 
      self.bucket.delete_key(aws_key)

def upload_static_to_aws(sync_dir):
  """ Copy the static diretory to aws.

  """
  ss = StaticSync(sync_dir)
  # Happens only once
  target = '{0}/MD5'.format(sync_dir)
  if not os.path.exists(target):
    f = open(target, 'wb')
    current_md5s = ss.get_md5s()
    ss.sync_s3_files(current_md5s.keys())
    fw = open(target, 'w')
    fw.write(json.dumps(current_md5s))
    fw.close()

  # Read in md5's of all files and only upload the ones that have changed
  changed_files = []
  remove_files = []
  current_md5s = ss.get_md5s()

  fr = open(target, 'r')
  lines = fr.readlines()
  if not lines:
    changed_files = current_md5s.keys()
  else:
    past_md5s = json.loads(lines[0])
    # Get missing files
    missing_files = list(set(current_md5s.keys()) - set(past_md5s.keys()))
    remove_files = list(set(past_md5s.keys()) - set(current_md5s.keys()))
    for key, val in current_md5s.iteritems():
      if past_md5s.get(key) != val:
        missing_files.append(key)
    for key in missing_files:
      changed_files.append(key)

  if target in changed_files:
    changed_files.remove(target)

  ss.sync_s3_files(changed_files, remove_files)

  # Remove any files 
  fw = open(target, 'w')
  fw.write(json.dumps(current_md5s))


if __name__ == '__main__':
  try:
    sync_dir = sys.argv[1]
  except IndexError:
    print 'Please provide a valid directory'
    exit()

  if not os.path.exists(sync_dir):
    print 'Please provide a valid directory'
  else:
    upload_static_to_aws(sync_dir)

