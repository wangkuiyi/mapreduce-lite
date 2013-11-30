#!/usr/bin/env python
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------#
# Copyright 2010 Tencent Inc.
# Author: Zhihui JIN <rickjin@tencent.com>
#
#----------------------------------------------------------------------------#

""" Mapreduce-lite job scheduler, given the description of map workers and
reduce workers through command line options, the schduler will do the following
jobs:
1. Copy the binary executable and some python scripts to a temporary directory
in each machine, the temporary directory is specified by user

2. Start communictors in each machine by running python script 'worker.py',
which will establish communication among machines

3. Start map worker or reduce worker for each machine

NOTE:
1. Two kinds of log file will be produced during processing, the log files of
map worker and reduce worker, produced by the binary executable,  will be stored
in the path specified by option '--mapreduce_log_filebase'; the log file of
scheduler, produced by python scripts, will be stored in path corresponding to
option '--mapreduce_tmp_dir'

2. In each command line option, both IP or machine name can be used when you
want to specify a machine. Intertally machine names will be converted to IPs
by looking up the config file '/etc/hosts'. So if you use machine names, please
make sure that the mapping from machine name to IPs is well configed in
'/etc/hosts'

3. Internally we use 'scp' and 'ssh' command, and we constantly set the BASH
environment variable 'PATH' as follows:
os.environ['PATH'] = '/usr/local/bin:/bin:/usr/bin:/sbin/'
Please make sure that the command 'scp', 'ssh' can be found in above paths.

4. Special characters, such as semicolon(;), colon(:) are forbidden
in file path string

5. So far, the Python scripts are tested on Python 2.4, 2.6 and 3.2

"""
import logging
import os
import pickle
import signal
import socket
import subprocess
import sys
import traceback
import time

from worker import SCRIPT_WORKER, ALL_SCRIPTS, Communicator
from util import CmdTool, SocketWrapper, config_logging
from mrlite_options import MRLiteOptionParser

#----------------------------------------------------------------------------#
# Job scheduler
#----------------------------------------------------------------------------#
class MRLiteJobScheduler(CmdTool):
    """ Job scheduler of map workers and reduce workers
    """
    def __init__(self, options):
        """ Copy python script worker.py and user-specified executable
        to specified directory on remote machines
        """
        CmdTool.__init__(self, options.mapreduce_ssh_port)
        self.options= options
        self.all_socks = []
        self.reduce_socks = None

        # copy map-reduce executable and worker script to each machine
        cmd = 'cp %s %s/%s' %(options.local_executable, 
                              options.script_path,
                              options.remote_executable)
        self.run_cmd_and_wait(cmd)
        executable = '%s/%s' %(options.script_path, options.remote_executable)
        file_list = [executable]
        for script in ALL_SCRIPTS:
            local_script = '%s/%s' %(options.script_path, script)
            file_list.append(local_script)
        mesg = 'Copy binary executable and python scripts to machines'
        logging.info(mesg)
        self.dispatch_file(file_list, options.mapreduce_tmp_dir)
        self.run_cmd_and_wait('rm -rf %s' %executable)

    def start_communicators(self):
        """ Start the server socket where the scheduler locates, and start all
        communicators, each communicator will connected to the server socket.
        The communicators will be used to control map/reduce workers.
        """
        options = self.options

        # start server
        server_ip = options.mapreduce_scheduler_ip
        server_sockobj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sockobj.bind((server_ip, 0))
        server_sockobj.listen(5)
        temp_ip, server_port = server_sockobj.getsockname()

        # start client
        for rank in range(options.num_worker):
            task = options.all_tasks[rank]
            machine = task['machine']
            tmp_dir = task['tmp_dir']
            worker_script = '%s/%s' %(tmp_dir, SCRIPT_WORKER)
            worker_cmd = 'python %s -s %s -p %s -r %s -d %s' %(
                worker_script,
                server_ip, server_port,
                rank, tmp_dir)
            self.run_ssh_cmd(machine, worker_cmd)

        #send global options to each communicator
        pickled_options =  pickle.dumps(options)
        all_socks = [None] * options.num_worker
        all_pids = [None] * options.num_worker
        for i in range(options.num_worker):
            connection, address = server_sockobj.accept()
            sock = SocketWrapper(connection)
            mesg, rank, pid = sock.recv().split()
            if mesg == 'rank':
                mesg = 'Socket communicator %s started at %s pid=%s' %(
                    rank, address, pid)
                logging.debug(mesg)
            sock.send(pickled_options)
            rank = int(rank)
            all_socks[rank] = sock
            all_pids[rank] = int(pid)
        self.map_socks = all_socks[0:options.num_map_worker]
        self.reduce_socks = all_socks[options.num_map_worker:]
        self.all_socks = all_socks
        self.all_pids = all_pids

    def start_workers(self):
        options = self.options
        reduce_mesg = 'Start %s reduce workers' %options.num_reduce_worker
        map_mesg = 'Start %s map workers' %options.num_map_worker

        if options.mapreduce_incremental_mode:
            logging.info(reduce_mesg)
            self.start_reduce_workers()
            for i in range(options.num_reduce_worker):
                mesg = self.reduce_socks[i].recv()
                assert(mesg == 'reducer_started')
            logging.info(map_mesg)
            self.start_map_workers()
        else:
            logging.info(map_mesg)
            self.start_map_workers()
            for i in range(options.num_map_worker):
                mesg = self.map_socks[i].recv()
                assert(mesg == 'mapper_finished')
            logging.info(reduce_mesg)
            self.start_reduce_workers()

    def start_map_workers(self):
        """ Start map workers
        """
        options = self.options
        for i in range(options.num_map_worker):
            instruction = 'start_mapper'
            self.map_socks[i].send(instruction)

    def start_reduce_workers(self):
        """ Start reduce workers
        """
        options = self.options
        for i in range(options.num_reduce_worker):
            instruction = 'start_reducer'
            self.reduce_socks[i].send(instruction)

    def start_jobs(self):
        """ Start communicators and map/reduce workers
        """
        if self.options.mapreduce_incremental_mode:
            mode = 'INCREMENTAL'
        else:
            mode = 'BATCH'
        logging.info('Mapreduce-Lite begin to work in %s mode' %mode)
        logging.info('Start socket communicators')
        self.start_communicators()
        self.start_workers()

    def get_worker_name(self, rank):
        """ Get the name of worker with specified rank
        """
        all_tasks = self.options.all_tasks
        assert(rank <= len(all_tasks))
        if rank < self.options.num_map_worker:
            type = 'Mapper'
            local_rank = rank 
        else:
            type = 'Reducer'
            local_rank = rank - self.options.num_map_worker
        task = all_tasks[rank]
        name = '%s-%s(%s, %s)' %(type, local_rank, 
                                 task['machine'], task['class'])
        return name

    def monitor_jobs(self):
        """ Monitor all runinng map workers and reduce workers, lookup status
        of each worker every 2 seconds, return if all workers quit
        """
        all_socks = self.all_socks
        running_jobs = set(range(len(all_socks)))
        while True:
            finished_jobs = set()
            for i in running_jobs:
                all_socks[i].send('status')
                mesg = all_socks[i].recv()
                if mesg == 'Finished':
                    finished_jobs.add(i)
                elif mesg == 'Failed':
                    mesg = '%s Failed' %self.get_worker_name(i)
                    raise Exception(mesg)

            for i in finished_jobs:
                mesg = '%s finished well' %self.get_worker_name(i)
                logging.debug(mesg)
                running_jobs.remove(i)
            if len(running_jobs) == 0:
                break

            # time interval for job monitoring is 5 seconds
            time.sleep(5)

    def quit_jobs(self):
        """ Normally quit all workers
        """
        for sock in self.all_socks:
            sock.send('quit')

    def kill_jobs(self):
        """ Kill all workers
        """
        for rank in range(self.options.num_worker):
            task = self.options.all_tasks[rank]
            machine = task['machine']
            pid = self.all_pids[rank]
            worker_cmd = 'kill -15 %s >/dev/null 2>&1' %pid
            self.run_ssh_cmd(machine, worker_cmd)

#----------------------------------------------------------------------------#
# MODULE EPILOGUE
#----------------------------------------------------------------------------#

def main(argv):
    """ The main method for this module.
    """
    logfile = os.path.join(os.getcwd(), 'log-mrlite-scheduler.txt')
    config_logging(logfile)
    start_time = time.time()
    logging.info('Job started at %s' %time.asctime())
    parser = MRLiteOptionParser()
    options, args = parser.parse_args(argv)
    scheduler = MRLiteJobScheduler(options)
    try:
        scheduler.start_jobs()
        scheduler.monitor_jobs()
        scheduler.quit_jobs()
        logging.info('Job finished at %s' %time.asctime())
        logging.info('Job run for %.3f seconds' %(time.time() - start_time))
    except KeyboardInterrupt:
        logging.info('Interrupted by CTRL-C')
        scheduler.kill_jobs()
        sys.exit(-1)
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        mesg = ''.join(traceback.format_exception(exc_type, exc_value,
                                                  exc_traceback))
        logging.info('cache exception in mrlite.py')
        logging.info(mesg)
        scheduler.kill_jobs()
        sys.exit(-1)

#----------------------------------------------------------------------------#
if __name__ == '__main__':
    main(sys.argv[1:])
