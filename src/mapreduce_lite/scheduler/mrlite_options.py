#!/usr/bin/env python
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------#
# Copyright 2010 Tencent Inc.
# Author: Zhihui JIN <rickjin@tencent.com>
#
#----------------------------------------------------------------------------#

""" Command line option parser for mapreduce-lite
"""
import datetime
import os
import re
import socket
import sys
import textwrap
import traceback
import getpass
try: # for python version < 3.0
    from ConfigParser import ConfigParser
except:
    from configparser import ConfigParser
from optparse import OptionParser, make_option, IndentedHelpFormatter

#----------------------------------------------------------------------------#
# Parse command line options
#----------------------------------------------------------------------------#

# Command line options definition
mrlite_option_list = [
make_option("--mapreduce_cmd",
            help="Command to run mapreduce task, usually a binary excecutable "
            "with its command line options"),

make_option("--mapreduce_map_io",
            help="Input and ouptut of map workers, descriptions of map "
            "workers are seperated by semicolon(;). For each map worker, "
            "you need to specify five fields seperated by colon(:), "
            "including:\n"
            "<machines>       machine list to run map worker\n"
            "<mapper_class>   class of map worker\n"
            "<input_format>   either 'text' or 'recordio'\n"
            "<input_path>     file pattern for input files\n"
            "<output_path>    output directory for storing\n"
            "                 intermediate result"
           ),

make_option("--mapreduce_reduce_io",
            help="Input and ouptut of reduce workers, descriptions of "
            "reduce workers are seperated by semicolon(;). For each reduce "
            "worker, you need to specify five fields seperated by colon(:),"
            "including:\n"
            "<machines>       machine list to run reduce worker\n"
            "<reduce_class>   class of reduce worker\n"
            "<input_path>     reduce input directory for storing\n"
            "                 output of map workers\n"
            "<output_format>  either 'text' or 'recordio'\n"
            "<output_path>    output directory\n"
            ),

make_option("--mapreduce_maponly_map_io",
            help="Input and output of map workers, for MAP-ONLY mode. "
            "Similar to option 'mapreduce_map_io, but differ in that the "
            "description of each map-only map worker consists of six "
            "fields, inlcuding\n"
            "<machines>       machine list to run map worker\n"
            "<mapper_class>   class of map worker\n"
            "<input_format>   either 'text' or 'recordio'\n"
            "<input_path>     file pattern for input files\n"
            "<output_format>  either 'text' or 'recordio'\n"
            "<output_path>    output directory"
           ),

make_option("--mapreduce_log_filebase", default="/tmp/mrlite/log",
            help="Filebase for log file. Each map worker and reduce worker "
            "will produce a log file, all log filenames begin with the "
            "specified filebase prefix"),

make_option("--mapreduce_tmp_dir", default='/tmp/mrlite',
            help="Directory in each machine to store temporary "
            "files, including the binary executable, python scripts of "
            "scheduler, log of scheduler, etc."),

make_option("--mapreduce_buffer_size", default=1024, type="int",
            help="Maximal memory buffer size(MB) for each map worker, "
            "default(1024)"),

make_option("--mapreduce_incremental_mode", action="store_true",
            default=False,
            help= "By default, MapReduce Lite works in batch reduction mode, "
            "the one compatible with Google MapReduce and Hadoop. Or, it can "
            "work in the incremental reduction mode, which is a specific "
            "function provided by MapReduce Lite for fast reduction. "
            "Please refer to 'mapreduce_lite.h' for more details"),

make_option("--mapreduce_force_mkdir", action="store_true",
            default=False,
            help= "By default, the input and output directory of each worker "
            "should be created by users. If this option is given, the "
            "directory of each worker will be created automatically when "
            "necessary. Make sure you don't misspell the path when you turn "
            "on this option "),

make_option("--mapreduce_ssh_port", default=22, type="int",
            help="SSH communication port, default(22)"),
]

# Version message
mrlite_version = """Mapreduce-lite 1.0.0
Copyright 2010 Tencent Inc.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Written by
  Yi WANG <yiwang@tencent.com>
  Hao YAN <charlieyan@tencent.com>
  Zhihui JIN <rickjin@tencent.com>
"""

# Usage help message
mrlite_usage = """Usage: %prog binary-excecutable [options]
Job scheduler for Mapreduce-lite

Given the description of map workers and reduce workers through command line
options, the schduler will do the following jobs:

1. Copy the binary executable and some python scripts to a temporary directory
in each machine, the temporary directory is specified by user

2. Start communictors in each machine by running python script 'worker.py',
which will establish communication among machines

3. Start map worker or reduce worker in each machine

Example:

$> ./%prog wordcount
--mapreduce_map_io="{10.1.149.174}:WordCountMapper:text:/data/text-00?:/tmp/;
                    {10.1.149.175}:WordCountMapper:text:/data/news-*11:/tmp/;"
--mapreduce_reduce_io="{10.1.149.174}:WordCountReducer:/tmp/:text:/tmp/output"
--mapreduce_tmp_dir="{10.1.149.174, 10.1.149.175}/tmp/"
--mapreduce_log_filebase="{10.1.149.174, 10.1.149.175}/tmp/log"
--mapreduce_buffer_size=1024
--mapreduce_ssh_port=36000

See mrlite_options_test.py for more examples
"""


class IndentedHelpFormatterWithNL(IndentedHelpFormatter):
    """ A help message formatter which will not filter the newline,
    copied from
    http://groups.google.com/group/comp.lang.python/
    browse_frm/thread/6df6e6b541a15bc2/09f28e26af0699b1?pli=1
    """
    def format_option(self, option):
        """ Keep newline in option message
        """
        result = []
        opts = self.option_strings[option]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if option.help:
            help_text = self.expand_default(option)
            help_lines = []
            for para in help_text.split("\n"):
                help_lines.extend(textwrap.wrap(para, self.help_width))
            result.append("%*s%s\n" % (
                indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line)
                           for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)


class MRLiteOptionParser(OptionParser):
    """ Parser for the mrlite command line options.
    """
    def __init__(self, debug=False):
        OptionParser.__init__(self, usage=mrlite_usage, version=mrlite_version,
                              formatter=IndentedHelpFormatterWithNL())
        self.debug = debug
        for option in mrlite_option_list:
            self.add_option(option)

    def pre_check_validity(self, options):
        """ Check validity of options
        """
        if options.mapreduce_maponly_map_io:
            if options.mapreduce_map_io or options.mapreduce_reduce_io:
                mesg = ("--mapreduce_map_io and --mapreduce_reduce_io are"
                       " forbidden in map_only mode")
                self.error_exit(mesg)
        else:
            if not options.mapreduce_map_io or not options.mapreduce_reduce_io:
                mesg = ("--mapreduce_map_io and --mapreduce_reduce_io "
                        "should be provided in normal mode")
                self.error_exit(mesg)
            if not options.mapreduce_cmd:
                mesg = ("--mapreduce_cmd is required")
                self.error_exit(mesg)

    def parse_args(self, argv):
        """ Parse command line options
        """
        if not argv:
            self.print_help()
            sys.exit(-1)

        options, args = OptionParser.parse_args(self, argv)
        self.pre_check_validity(options)
        if options.mapreduce_incremental_mode:
            batch_mode = 'false'
        else:
            batch_mode = 'true'
        maponly_mode = bool(options.mapreduce_maponly_map_io)
        options.ensure_value('maponly_mode', maponly_mode)
        options.ensure_value('batch_mode', batch_mode)

        fields = options.mapreduce_cmd.split(' ', 1)
        if len(fields) == 1:
            local_executable = fields[0]
            cmd_args = ''
        else:
            local_executable, cmd_args = fields
        local_executable = os.path.normpath(local_executable)
        identity = self.get_identity(local_executable)
        script_path = os.path.abspath(os.path.dirname(__file__))
        options.ensure_value('identity', identity)
        options.ensure_value('script_path', script_path)
        options.ensure_value('local_executable', local_executable)
        options.ensure_value('cmd_args', cmd_args)
        options.ensure_value('remote_executable', identity)
        options.ensure_value('machines', set())
        options.ensure_value('mapreduce_scheduler_ip', self.get_host_ip())

        self.parse_tmp_dir(options)
        self.parse_log_filebase(options)
        if maponly_mode:
            self.parse_maponly_map_task(options)
            num_worker = options.num_map_worker
            all_tasks = options.map_tasks
        else:
            self.parse_map_task(options)
            self.parse_reduce_task(options)
            num_worker = options.num_map_worker + options.num_reduce_worker
            all_tasks = options.map_tasks + options.reduce_tasks
        options.ensure_value('num_worker', num_worker)
        options.ensure_value('all_tasks', all_tasks)
        self.post_check_validity(options)
        self.options = options
        return options, args

    def parse_tmp_dir(self, options):
        """ Parse temporary directory for each machine, the map workers on one
        machine will share the same directory
        """
        result = {}
        regex = "\{(?P<machines>[-_., a-zA-Z0-9]+)\}(?P<output_path>[^;@]+)"
        pattern_tmp_dir = re.compile(regex)
        fields = self.split_fields(options.mapreduce_tmp_dir, ';')
        for tmp_dir_str in fields:
            m = pattern_tmp_dir.match(tmp_dir_str)
            for machine in self.split_fields(m.group('machines'), ','):
                result[machine] = os.path.normpath(m.group('output_path'))
        options.mapreduce_tmp_dir =  result

    def parse_log_filebase(self, options):
        """ Parse log filebase for each machine, the workers on one
        machine will share the same filebase
        """
        result = {}
        regex = "\{(?P<machines>[ -_.,a-zA-Z0-9]+)\}(?P<output_path>[^;@]+)"
        pattern_log_filebase = re.compile(regex)
        fields = self.split_fields(options.mapreduce_log_filebase, ';')
        for log_filebase_str in fields:
            m = pattern_log_filebase.match(log_filebase_str)
            for machine in self.split_fields(m.group('machines'), ','):
                result[machine] = m.group('output_path')
        options.mapreduce_log_filebase =  result

    def parse_machines(self, str):
        """ Parse machine list from str, and convert machine names to ips
        """
        assert(str[0] == '{')
        assert(str[-1] == '}')
        str = str[1:-1]
        machines = self.split_fields(str, ',')
        assert(len(machines) > 0)
        machine_ips = self.get_machine_ip(machines)
        return machine_ips

    def convert_task(self, task):
        """ Convert representation of task from list-form to dict-form
        """
        keys = ['machine',
                'class',
                'input_format',
                'input_path',
                'output_format',
                'output_path',
                'tmp_dir',
                'log_filebase']
        result = {}
        assert(len(keys) == len(task))
        for i in range(len(keys)):
            result[keys[i]] = task[i]
        return result

    def parse_map_task(self, options):
        """ Parse option --mapreduce_map_io and generate map tasks
        """
        map_tasks = []
        machine_set = set()
        task_list = self.split_fields(options.mapreduce_map_io, ';')
        for task_str in task_list:
            fields = self.split_fields(task_str, ':')
            assert(len(fields) == 5)
            (machine_str, class_name, input_format,
             input_path, output_path) = fields
            machines = self.parse_machines(machine_str)
            machine_set.update(set(machines))
            output_format  = None
            for machine in machines:
                tmp_dir = options.mapreduce_tmp_dir[machine]
                log_filebase = options.mapreduce_log_filebase[machine]
                task = [machine, class_name, input_format, input_path,
                        output_format, output_path, tmp_dir, log_filebase]
                task = self.convert_task(task)
                map_tasks.append(task)
        options.ensure_value('map_tasks', map_tasks)
        options.ensure_value('map_machines', machine_set)
        options.ensure_value('num_map_worker', len(map_tasks))
        options.machines.update(machine_set)

    def parse_maponly_map_task(self, options):
        """ Parse option --mapreduce_maponly_map_io and generate map-only tasks
        """
        map_tasks = []
        machine_set = set()
        task_list= self.split_fields(options.mapreduce_maponly_map_io, ';')
        for task_str in task_list:
            fields = self.split_fields(task_str, ':')
            assert(len(fields) == 6)
            (machine_str, class_name, input_format,
             input_path, output_format, output_path) = fields
            machines = self.parse_machines(machine_str)
            machine_set.update(set(machines))
            for machine in machines:
                tmp_dir = options.mapreduce_tmp_dir[machine]
                log_filebase = options.mapreduce_log_filebase[machine]
                task = [machine, class_name, input_format, input_path,
                        output_format, output_path, tmp_dir, log_filebase]
                task = self.convert_task(task)
                map_tasks.append(task)
        options.ensure_value('map_tasks', map_tasks)
        options.ensure_value('map_machines', machine_set)
        options.ensure_value('num_map_worker', len(map_tasks))
        options.machines.update(machine_set)

    def parse_reduce_task(self, options):
        """ Parse option --mapreduce_reduce_io, and generate reduce tasks
        """
        reduce_tasks = []
        machine_set = set()
        task_list = self.split_fields(options.mapreduce_reduce_io, ';')
        for task_str in task_list:
            fields = self.split_fields(task_str, ':')
            assert(len(fields) == 5)
            (machine_str, class_name,
             input_path, output_format, output_path) = fields
            input_format = None
            machines = self.parse_machines(machine_str)
            machine_set.update(set(machines))
            for machine in machines:
                tmp_dir = options.mapreduce_tmp_dir[machine]
                log_filebase = options.mapreduce_log_filebase[machine]
                task = [machine, class_name, input_format, input_path,
                        output_format, output_path, tmp_dir,  log_filebase]
                task = self.convert_task(task)
                reduce_tasks.append(task)

        # get the string of all reduce workers
        num_reduce_worker = len(reduce_tasks)
        reduce_ports = self.get_free_ports(num_reduce_worker)
        reduce_workers = []
        for i in range(num_reduce_worker):
            reducer = "%s:%s" %(reduce_tasks[i]['machine'], reduce_ports[i])
            reduce_workers.append(reducer)

        options.ensure_value('reduce_tasks', reduce_tasks)
        options.ensure_value('reduce_workers', ','.join(reduce_workers))
        options.ensure_value('num_reduce_worker', num_reduce_worker)
        options.ensure_value('reduce_machines', machine_set)
        options.machines.update(machine_set)

    def error_exit(self, mesg):
        """ Print message and quit
        """
        mesg = 'ERROR: %s' % mesg
        print(mesg)
        sys.exit(-1);

    def post_check_validity(self, options):
        """Check basic validity of options
        """
        if not os.path.exists(options.local_executable):
            mesg = 'file %s does not exists' %options.local_executable
            self.error_exit(mesg)

        for machine in options.mapreduce_tmp_dir:
            if not machine in options.machines:
                mesg = 'illegal machine %s in mapreduce_tmp_dir' %machine
                self.error_exit(mesg)

        for machine in options.mapreduce_log_filebase:
            if not machine in options.machines:
                mesg = 'illegal machine %s in mapreduce_log_filebase' %machine
                self.error_exit(mesg)

        formats = [None, 'text', 'recordio']
        for i in range(options.num_worker):
            task = options.all_tasks[i]
            assert(task['machine'])
            assert(task['class'])
            assert(task['input_path'])
            assert(task['output_path'])
            assert(task['tmp_dir'])
            assert(task['log_filebase'])
            assert(task['input_format'] in formats)
            assert(task['output_format'] in formats)

    def get_identity(self, local_executable):
        """ Get identity of job
        """
        user = getpass.getuser()
        host = socket.gethostname()
        pid  = str(os.getpid())
        time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
        executable  = os.path.basename(local_executable)
        identity = '-'.join([executable, user, time])
        return identity

    def split_fields(self, s, sep):
        fields = map(str.strip, s.split(sep))
        return [f for f in fields if f]

    def is_ip_address(self, input):
        """ Whether a string is an ip address
        """
        if self.debug:
            return True

        fields = input.split('.')
        if len(fields) != 4:
            return False

        for field in fields:
            if not (field.isdigit() and field[0] != '0' and int(field) < 256):
                return False
        return True

    def get_machine_ip(self, machines):
        """ Return the ip addrees of machines
        """
        machine_ip_list = []
        for machine in machines:
            if self.is_ip_address(machine):
                machine_ip_list.append(machine)
            else:
                try:
                    ip = socket.gethostbyname(machine)
                    machine_ip_list.append(ip)
                except socket.error:
                    tb_mesg = '\n'.join(map(str, traceback.extract_stack()))
                    mesg = ('fail to convert machine name %s to ip: %s'
                            %(machine, tb_mesg))
                    self.error_exit(mesg)
        return machine_ip_list

    def get_free_ports(self, num):
        """ Get avaialable ports
        """
        free_ports = []
        sock_list = []
        for i in range(num):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('127.0.0.1', 0))
            sock_list.append(sock)

        for sock in sock_list:
            ip, port = sock.getsockname()
            free_ports.append(port)
            sock.close()
        return free_ports

    def get_host_ip(self):
        """ There are two ways to parse the ip of current machine
         1. ip = socket.gethostbyname(host_name)
            which is dependent on the config file '/etc/hosts'.
         2. ip = commands.getoutput("ifconfig").split("\n")[1].split()[1][5:]
            use the output of command 'ifconfig'
        """
        server_name = socket.gethostname()
        server_ip = socket.gethostbyname(server_name)
        return server_ip


