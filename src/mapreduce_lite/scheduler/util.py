#!/usr/bin/env python
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------#
# Copyright 2010 Tencent Inc.
# Author: Zhihui JIN <rickjin@tencent.com>
#
#----------------------------------------------------------------------------#

""" utilities for mapreduce-lite
"""
import inspect
import logging
import os
import sys
import subprocess
import traceback
import urllib

#----------------------------------------------------------------------------#
# Utilities
#----------------------------------------------------------------------------#

def config_logging(logfile):
    """ Config global logging interface
    all messages will be output to log file, the messages with level >=
    logging.INFO will be output console
    """
    format_str = ("%(levelname)-8s %(asctime)s %(filename)s:%(lineno)d"
                  " ] %(message)s")
    logging.basicConfig(filename=logfile, filemode='w',
                        format=format_str, level=logging.DEBUG)
    logger = logging.getLogger()
    format_str = ("%(filename)s: %(message)s")
    formatter = logging.Formatter(format_str)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

class SocketWrapper(object):
    """ Socket wrapper
    """
    def __init__(self, sockobj):
        self.buf = ''
        self.sockobj = sockobj

    def close(self):
        self.sockobj.close()

    def send(self, mesg):
        """ Send out message
        """
        mesg =  urllib.quote(mesg)
        mesg = '%s\n' %mesg
        self.sockobj.sendall(mesg)

    def recv(self):
        """ Receive message
        """
        while not '\n' in self.buf:
            self.buf += self.sockobj.recv(1024)
        message, remain = self.buf.split('\n', 1)
        self.buf = remain
        return urllib.unquote(message)

class CmdTool(object):
    """ Command line tool, used to
    1. run command in local Bash
    2. run ssh command on remote machine
    3. copy files to remote machines via scp
    """
    def __init__(self, ssh_port=22):
        self.ssh_port = ssh_port

    def display(self, mesg):
        """ dispaly message and caller
        """
        caller = '<%s.%s>' %(self.__class__.__name__, inspect.stack()[1][3])
        mesg = '%s: %s' %(caller, mesg)
        print(mesg)
        sys.stdout.flush()

    def set_ssh_port(self, ssh_port):
        self.ssh_port = ssh_port

    def wait_cmd(self, process, cmd_str):
        """ wait for the process running cmd
        """
        retcode = process.wait()
        if retcode != 0:
            mesg = 'Fail with retcode(%s): %s' %(retcode, cmd_str)
            raise RuntimeError(mesg)

    def run_cmd(self, cmd_str):
        """ Run local command
        """
        cmd_lines = [line for line in cmd_str.splitlines() if len(line) > 0]
        cmd_str = ' \\\n'.join(cmd_lines)
        os.environ['PATH'] = '/usr/local/bin:/bin:/usr/bin:/sbin/'
        process = subprocess.Popen(cmd_str, shell=True, env=os.environ)
        mesg = 'run command PPID=%s PID=%s CMD=%s' %(os.getpid(), process.pid, cmd_str)
        logging.debug(mesg)
        return process

    def run_cmd_and_wait(self, cmd_str):
        process = self.run_cmd(cmd_str)
        self.wait_cmd(process, cmd_str)

    def run_ssh_cmd(self, machine, remote_cmd):
        """ Run command via SSH in remote machines
        """
        ssh = 'ssh -q -p %s' % self.ssh_port
        ssh_cmd = '%s %s \'%s\'' %(ssh, machine, remote_cmd)
        return self.run_cmd(ssh_cmd)

    def dispatch_file(self, file_list, dir_dict):
        """ Copy source files to remote machines
        """
        files = ' '.join(file_list)
        job_list = []
        for machine,tmp_dir in dir_dict.items():
            cmd_mkdir = 'mkdir -p %s' % tmp_dir
            job_list.append([self.run_ssh_cmd(machine, cmd_mkdir),
                            cmd_mkdir])
        for process, cmd_scp in job_list:
            self.wait_cmd(process, cmd_scp)

        job_list = []
        for machine,tmp_dir in dir_dict.items():
            cmd_scp = ('scp -q -P %s %s %s:%s/ >/dev/null'
                       % (self.ssh_port, files, machine, tmp_dir))
            job_list.append([self.run_cmd(cmd_scp), cmd_scp])
        for process, cmd_scp in job_list:
            self.wait_cmd(process, cmd_scp)

