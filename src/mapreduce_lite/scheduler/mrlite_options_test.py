#!/usr/bin/env python
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------#
# Copyright 2010 Tencent Inc.
# Author: Zhihui JIN <rickjin@tencent.com>
#
#----------------------------------------------------------------------------#

""" unittest for mrlite_options
"""
import os
import pickle
import sys
import unittest
from mrlite_options import MRLiteOptionParser

class TestMRLiteOptionParser(unittest.TestCase):

    def test_normal_case(self):
        argv = [
         "--mapreduce_cmd=/bin/echo --args1 --args2",
         "--mapreduce_map_io=\
            {m1, m2}:WordCountMapper:recordio : /input-000??-of-00005:/tmp/output;\
            {m3}:BigramCountMapper:text:/text-* :/tmp/output",

         "--mapreduce_reduce_io=\
            {m1, m2}:WordCountReducer:/disk10/reduce-input: text:/disk10/output;\
            {m2,m3}:IdentityReducer: /disk7/reduce-input:recordio:/disk7/output",

         "--mapreduce_log_filebase={m1, m2}/disk1/log;{ m3}/disk3/tmp/log", 
         "--mapreduce_tmp_dir={m1,m2}/disk1/tmp/;{m3 }/tmp",
         "--mapreduce_incremental_mode",
         "--mapreduce_ssh_port=36000",
         ]
  
        expected_machines = set(['m1', 'm2', 'm3'])
        expected_map_tasks = [
            {'machine':        'm1',
             'class':          'WordCountMapper',
             'input_format':   'recordio',
             'input_path':     '/input-000??-of-00005',
             'output_format':  None,
             'output_path':    '/tmp/output',
             'tmp_dir':        '/disk1/tmp',
             'log_filebase':   '/disk1/log'},

            {'machine':        'm2',
             'class':          'WordCountMapper',
             'input_format':   'recordio',
             'input_path':     '/input-000??-of-00005',
             'output_format':  None,
             'output_path':    '/tmp/output',
             'tmp_dir':        '/disk1/tmp',
             'log_filebase':   '/disk1/log'},

            {'machine':        'm3',
             'class':          'BigramCountMapper',
             'input_format':   'text',
             'input_path':     '/text-*',
             'output_format':  None,
             'output_path':    '/tmp/output',
             'tmp_dir':        '/tmp',
             'log_filebase':   '/disk3/tmp/log'} 
        ]
        expected_reduce_tasks = [
            {'machine':        'm1',
             'class':          'WordCountReducer',
             'input_format':   None,
             'input_path':     '/disk10/reduce-input',
             'output_format':  'text',
             'output_path':    '/disk10/output',
             'tmp_dir':        '/disk1/tmp',
             'log_filebase':   '/disk1/log'},

            {'machine':        'm2',
             'class':          'WordCountReducer',
             'input_format':   None,
             'input_path':     '/disk10/reduce-input',
             'output_format':  'text',
             'output_path':    '/disk10/output',
             'tmp_dir':        '/disk1/tmp',
             'log_filebase':   '/disk1/log'},

            {'machine':        'm2',
             'class':          'IdentityReducer',
             'input_format':   None,
             'input_path':     '/disk7/reduce-input',
             'output_format':  'recordio',
             'output_path':    '/disk7/output',
             'tmp_dir':        '/disk1/tmp',
             'log_filebase':   '/disk1/log'},

            {'machine':        'm3',
             'class':          'IdentityReducer',
             'input_format':   None,
             'input_path':     '/disk7/reduce-input',
             'output_format':  'recordio',
             'output_path':    '/disk7/output',
             'tmp_dir':        '/tmp',
             'log_filebase':   '/disk3/tmp/log'},
        ]
        expected_tmp_dir = {
            'm1': '/disk1/tmp', 
            'm2': '/disk1/tmp',
            'm3': '/tmp'}
        expected_log_filebase = {
            'm1': '/disk1/log', 
            'm2': '/disk1/log',
            'm3': '/disk3/tmp/log'} 
  

        parser = MRLiteOptionParser(debug=True)
        options, args = parser.parse_args(argv)
        self.assertEqual(expected_machines, options.machines)
        self.assertEqual(expected_map_tasks, options.map_tasks)
        self.assertEqual(expected_reduce_tasks, options.reduce_tasks)
        self.assertEqual(expected_tmp_dir, options.mapreduce_tmp_dir)
        self.assertEqual(expected_log_filebase, options.mapreduce_log_filebase)
        self.assertEqual('/bin/echo', options.local_executable)
        self.assertEqual('--args1 --args2', options.cmd_args)

    def test_map_only_mode(self):
        argv = [
         "--mapreduce_cmd=/bin/echo",
         "--mapreduce_maponly_map_io=\
            {m1, m2}:WordCountMapper:recordio:/input-000??:text:/disk10/output;\
            {m3}:BigramCountMapper:text: /text-*:recordio:/disk7/output",
         "--mapreduce_log_filebase={m1,m2}/disk1/log; {m3}/disk3/tmp/log", 
         "--mapreduce_tmp_dir={m1,m2}/disk1/tmp/;{m3}/tmp;",
         ]
        parser = MRLiteOptionParser(debug=True)
        options, args = parser.parse_args(argv)
        expected_map_tasks= [
            {'machine':        'm1', 
             'class':          'WordCountMapper', 
             'input_format':   'recordio', 
             'input_path':     '/input-000??', 
             'output_format':  'text', 
             'output_path':    '/disk10/output', 
             'tmp_dir':        '/disk1/tmp', 
             'log_filebase':   '/disk1/log'},

            {'machine':        'm2',
             'class':          'WordCountMapper',
             'input_format':   'recordio',
             'input_path':     '/input-000??',
             'output_format':  'text',
             'output_path':    '/disk10/output',
             'tmp_dir':        '/disk1/tmp',
             'log_filebase':   '/disk1/log' },

            {'machine':        'm3',
             'class':          'BigramCountMapper',
             'input_format':   'text',
             'input_path':     '/text-*',
             'output_format':  'recordio',
             'output_path':    '/disk7/output',
             'tmp_dir':        '/tmp',
             'log_filebase':   '/disk3/tmp/log'}
        ]
        self.assertEqual(expected_map_tasks, options.map_tasks)

#----------------------------------------------------------------------------#
if __name__ == '__main__':
    unittest.main()
