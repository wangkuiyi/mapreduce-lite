#!/usr/bin/env python
# -*- coding: utf-8 -*-
#----------------------------------------------------------------------------#
# Copyright 2010 Tencent Inc.
# Author: Zhihui JIN <rickjin@tencent.com>
#
#----------------------------------------------------------------------------#

""" This script inlcudes implementation of different types of worker in
mapreduce_lite, as well as the socket communicator between worker machines
"""
import glob
import logging
import optparse
import os
import pickle
import signal
import socket
import sys
import subprocess
import time
import traceback

from util import CmdTool, SocketWrapper, config_logging

# all required scripts for each worker
SCRIPT_WORKER = 'worker.py'
SCRIPT_UTIL = 'util.py'
ALL_SCRIPTS = [SCRIPT_WORKER, SCRIPT_UTIL]


class Worker(CmdTool):
    """ Worker to do detailed tasks, we have three kinds of worker
        MapWorker
        MapOnlyWorker
        ReduceWorker
    """
    def __init__(self, options, rank, sock):
        CmdTool.__init__(self, options.mapreduce_ssh_port)
        self.options = options
        self.rank = rank
        self.process = None
        self.sock = sock
        if self.is_map_worker():
            type = 'Mapper'
            local_rank = rank 
        else:
            type = 'Reducer'
            local_rank = rank - options.num_map_worker
        task = options.all_tasks[rank]
        self.name = '%s-%s(%s, %s)' %(type,
                                      local_rank,
                                      task['machine'],
                                      task['class'])

    def is_map_worker(self):
        return self.rank < self.options.num_map_worker

    def is_reduce_worker(self):
        return self.rank >= self.options.num_map_worker

    def is_maponly_mode(self):
        return self.options.maponly_mode

    def is_batch_mode(self):
        return not self.options.mapreduce_incremental_mode

    def get_worker_cmd(self):
        mesg = 'Please reimplement get_worker_cmd in derived class'
        raise NotImplementedError(mesg)

class MapWorker(Worker):
    def __init__(self, options, rank, sock):
        Worker.__init__(self, options, rank, sock)
        logging.debug('I am a map worker with task rank %s' %rank)

    def start(self):
        """ Start to run the worker
        """
        logging.debug('%s started at %s' %(self.name, time.asctime()))
        cmd_str = self.get_worker_cmd()
        self.process = self.run_cmd(cmd_str)
        return self.process

    def wait(self):
        """ Wait for worker to finish
        """
        self.wait_cmd(self.process, self.get_worker_cmd())
        if self.is_batch_mode():
            self.push_reduce_buffers()
            logging.info('%s fininsed at %s' %(self.name, time.asctime()))
            time.sleep(0.5)
            self.sock.send('mapper_finished')

    def get_worker_cmd(self):
        """ Get commands for map wokers
        """
        options = self.options
        rank = self.rank
        reduce_input_buffer_size = (options.mapreduce_buffer_size /
                                    options.num_reduce_worker)
        map_worker_id = rank
        task  = options.all_tasks[rank]
        tmp_dir = task['tmp_dir']
        reduce_input_filebase = '%s/%s' %(task['output_path'], 
                                          options.identity)
        executable = '%s/%s %s' %(tmp_dir,
                                  options.remote_executable,
                                  options.cmd_args)
        param = (executable,
                 task['input_path'],
                 reduce_input_filebase,
                 options.batch_mode,
                 task['log_filebase'],
                 options.num_map_worker,
                 reduce_input_buffer_size,
                 options.reduce_workers,
                 map_worker_id,
                 task['class'],
                 task['input_format'])
        cmd_map_worker = """ %s
        --mr_input_filepattern="%s"
        --mr_reduce_input_filebase="%s"
        --mr_batch_reduction=%s
        --mr_log_filebase="%s"
        --mr_num_map_workers=%s
        --mr_reduce_input_buffer_size=%s
        --mr_reduce_workers=%s
        --mr_map_worker_id=%s
        --mr_map_only=false
        --mr_mapper_class=%s
        --mr_input_format=%s
        """ % param
        return cmd_map_worker

    def push_reduce_buffers(self):
        """ Move reduce buffer files to remote reducers,
        the filename example of buffer file is as follows:
            wordcount-user-time-mapper-00002-reducer-00000-00000000
        """
        options = self.options
        from_mapper_dir = options.all_tasks[self.rank]['output_path']
        from_mapper_machine = options.all_tasks[self.rank]['machine']
        mapper_id = '%05d' %self.rank
        pattern = '%s/%s-mapper-%s-reducer-*' %(from_mapper_dir,
                                                options.identity,
                                                mapper_id)
        input_buffer_list = glob.glob(pattern)
        #if len(input_buffer_list) == 0:
        #    raise RuntimeError('failed to find reduce buffers in mapper')

        for filename in input_buffer_list:
            fields = filename.rsplit('-', 4)
            reducer_id = int(fields[-2]) + options.num_map_worker
            assert(mapper_id == fields[-4])
            to_reducer_dir = options.all_tasks[reducer_id]['input_path']
            to_reducer_machine = options.all_tasks[reducer_id]['machine']
            logging.debug('push reduce buffer %s from %s to %s' %(
                filename, from_mapper_machine, to_reducer_machine))
            if to_reducer_machine == from_mapper_machine:
                if to_reducer_dir != from_mapper_dir:
                    cmd = 'mv %s %s' %(filename, to_reducer_dir)
                    self.run_cmd_and_wait(cmd)
            else:
                cmd = 'scp -q -P %s %s %s:%s >/dev/null' %(
                    options.mapreduce_ssh_port,
                    filename,
                    to_reducer_machine,
                    to_reducer_dir)
                self.run_cmd_and_wait(cmd)
                self.run_cmd_and_wait('rm -rf %s' %filename)

class MapOnlyWorker(Worker):
    def __init__(self, options, rank, sock):
        Worker.__init__(self, options, rank, sock)
        logging.debug('I am a map-only map worker with task rank %s' %rank)

    def start(self):
        """ Start to run the worker
        """
        logging.debug('%s started at %s' %(self.name, time.asctime()))
        cmd_str = self.get_worker_cmd()
        self.process = self.run_cmd(cmd_str)
        return self.process

    def wait(self):
        self.wait_cmd(self.process, self.get_worker_cmd())
        logging.info('%s fininsed at %s' %(self.name, time.asctime()))
        time.sleep(0.5)

    def get_worker_cmd(self):
        """ Get commands for map wokers
        """
        options = self.options
        rank = self.rank
        map_worker_id = rank
        task  = options.all_tasks[rank]
        executable = '%s/%s %s' %(task['tmp_dir'],
                                  options.remote_executable,
                                  options.cmd_args)
        param = (executable,
                 task['input_path'],
                 task['output_path'],
                 task['log_filebase'],
                 options.num_map_worker,
                 options.reduce_workers,
                 map_worker_id,
                 task['class'],
                 task['input_format'],
                 task['output_format'])
        cmd_map_worker = """ %s
        --mr_input_filepattern="%s"
        --mr_output_files="%s"
        --mr_log_filebase="%s"
        --mr_num_map_workers=%s
        --mr_reduce_workers=%s
        --mr_map_worker_id=%s
        --mr_map_only=true
        --mr_mapper_class=%s
        --mr_input_format=%s
        --mr_output_format=%s
        """ % param
        return cmd_map_worker


class ReduceWorker(Worker):
    def __init__(self, options, rank, sock):
        Worker.__init__(self, options, rank, sock)
        self.num_reduce_buffer = 0
        logging.debug('I am a reduce worker with task rank %s' %rank)

    def start(self):
        """ Start to run the worker
        """
        logging.debug('%s started at %s' %(self.name, time.asctime()))
        if self.is_batch_mode():
            self.prepare_reduce_buffers()
        cmd_str = self.get_worker_cmd()
        self.process = self.run_cmd(cmd_str)
        return self.process

    def wait(self):
        if self.is_batch_mode():
            self.wait_cmd(self.process, self.get_worker_cmd())
            logging.info('%s fininsed at %s' %(self.name, time.asctime()))
            time.sleep(0.5)
        else:
            # in incremental mode, reduce worker and map worker should work
            # parallelly, so we cann't wait the return of reduce worker. Also,
            # the reduce workers should started ahead of map workers
            # TODO{rickjin}: sleep 0.5 second is a heuristic solution to make
            # sure reduce workers are started before we start map workers, is
            # there a better way ?
            time.sleep(0.5)
            self.sock.send('reducer_started')

    def get_worker_cmd(self):
        """ Get commands for reduce workers
        """
        options = self.options
        rank = self.rank
        reduce_worker_id = self.rank - options.num_map_worker
        task  = options.all_tasks[rank]
        tmp_dir = task['tmp_dir']
        executable = '%s/%s %s' %(tmp_dir,
                                  options.remote_executable,
                                  options.cmd_args)
        reduce_input_filebase = '%s/%s' %(task['input_path'], 
                                          options.identity)
        param = (executable,
                 task['output_path'],
                 options.batch_mode,
                 reduce_input_filebase,
                 self.num_reduce_buffer,
                 task['log_filebase'],
                 options.num_map_worker,
                 options.reduce_workers,
                 reduce_worker_id,
                 task['class'],
                 task['output_format'])
        cmd_reduce_worker = """ %s
        --mr_output_files="%s"
        --mr_batch_reduction=%s
        --mr_reduce_input_filebase="%s"
        --mr_num_reduce_input_buffer_files=%s
        --mr_log_filebase="%s"
        --mr_num_map_workers=%s
        --mr_reduce_workers=%s
        --mr_reduce_worker_id=%s
        --mr_reducer_class=%s
        --mr_output_format=%s
        """ % param
        return cmd_reduce_worker

    def prepare_reduce_buffers(self):
        """ Rename reduce buffers
        """
        input_path= self.options.all_tasks[self.rank]['input_path']
        reducer_id = '%05d' %(self.rank - self.options.num_map_worker)
        pattern = '%s/%s-mapper-*-reducer-%s-*' %(input_path,
                                                  self.options.identity,
                                                  reducer_id)
        input_buffer_list = glob.glob(pattern)
        filenames = ' '.join(input_buffer_list)
        if len(input_buffer_list) == 0:
            raise RuntimeError('failed to find reduce buffers in reducer')
        logging.debug('pattern %s match %s buffer files: %s'
                      %(pattern, len(input_buffer_list), filenames))
        num = 0
        for filename in input_buffer_list:
            fields = filename.rsplit('-', 5)
            assert(reducer_id == fields[-2])
            mapper_id = int(fields[-4])
            newname = '%s-%010d' %(fields[0], num)
            self.run_cmd_and_wait('mv %s %s' %(filename, newname))
            num += 1
        self.num_reduce_buffer = num
        logging.debug('renamed %s buffer files' %num)


class Communicator(CmdTool):
    """ Communicators bewteen map/reduce workers and scheduler.
    For each worker, there will be a communicator connected to the job
    scheduler,  the communicator will be in charge of accepting instructions
    from the scheduler and executing task in each machine, including invoking
    map workers and reduce workers, monitoring the status of each worker and
    report it to the scheduler.

    """
    def __init__(self, server, port, rank, dir):
        """ Establish communication with scheduler
        """
        self.options = None
        logfile = os.path.join(dir, 'log-mrlite-rank-%s.txt' %rank)
        config_logging(logfile)
        logging.debug('communicator %s started in %s:%s at %s' %(
            rank, server, port, time.asctime()))
        sockobj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockobj.connect((server, port))
        sock = SocketWrapper(sockobj)
        sock.send('rank %s %s' %(rank, os.getpid()))
        pickled_options = sock.recv()
        options = pickle.loads(pickled_options)

        self.task_callback = {
            'start_mapper' : None,
            'start_reducer' : None,
            'status': self.report_status,
            'quit'  : self.quit,
            'exit'  : self.quit,
        }
        self.server = server
        self.rank = rank
        self.sock = sock
        self.options = options
        self.process = None
        if options.maponly_mode:
            self.worker = MapOnlyWorker(options, rank, sock)
        elif rank < options.num_map_worker:
            self.worker = MapWorker(options, rank, sock)
        else:
            self.worker = ReduceWorker(options, rank, sock)

    def run(self):
        """ Receive instructions from scheduler and run tasks
        """
        while True:
            instruction = self.sock.recv()
            mesg = 'worker %s accept instruction: %s' %(self.rank, instruction)
            logging.debug(mesg)
            self.run_instruction(instruction)

    def run_instruction(self, instruction):
        """ Run instruction given by scheduler
        """
        if instruction not in self.task_callback:
            raise RuntimeError('Illegal instruction %s' %instruction)

        if instruction == 'start_mapper' and self.worker.is_map_worker():
            self.process = self.worker.start()
            self.worker.wait()
        elif instruction == 'start_reducer' and self.worker.is_reduce_worker():
            self.process = self.worker.start()
            self.worker.wait()
        else:
            self.task_callback[instruction]()

    def kill_job(self):
        if not self.process:
            mesg = 'process is None'
            logging.debug(mesg)
            return
        if self.process.poll() is None:
            os.kill(self.process.pid, signal.SIGKILL)
            mesg = 'child mapreduce process %s is killed' %self.process.pid
            logging.debug(mesg)
        else:
            mesg = 'child mapreduce process %s is finisned' %self.process.pid
            logging.debug(mesg)

    def quit(self, retcode=0):
        self.sock.close()
        self.clean_tmp_files()
        logging.debug('worker quit with retcode=%s' %retcode)
        logging.debug('communicator %s finished at %s' %(
            self.rank, time.asctime()))
        sys.exit(retcode)

    def sigterm_handler(self, signum, frame):
        """ Quit when got SIGTERM signal from scheduler
        """
        logging.debug('Interrupted by SIGTERM')
        self.kill_job()
        self.quit(-1)

    def report_status(self):
        """ Report status of worker
        """
        if not self.process:
            return

        retval = self.process.poll()
        if retval is None:
            pid = self.process.pid
            cmd_str = 'top -p %s -b -n 1|grep -A 1 PID' % pid
            process = subprocess.Popen(cmd_str, shell=True,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            result = stdout.splitlines()
            if len(result) == 2:
                fields = result[1].split()
                mesg = 'Running %s %s' %(pid, ' '.join(fields[4:10]))
            else:
                mesg = 'Not-Sure'
        elif retval == 0:
            mesg = 'Finished'
        elif retval < 0:
            mesg = 'Failed'
        self.sock.send(mesg)

    def clean_tmp_files(self):
        """ Remove tmp files
        """
        options = self.options
        if not options:
            return
        tmp_dir = options.all_tasks[self.rank]['tmp_dir']
        logging.debug('clean temp files')
        rm_cmd = 'rm -rf %s/%s %s/%s %s/%s %s/*.pyc' %(
            tmp_dir, options.remote_executable,
            tmp_dir, SCRIPT_WORKER,
            tmp_dir, SCRIPT_UTIL,
            tmp_dir)
        self.run_cmd_and_wait(rm_cmd)

    def check_options(self):
        """ Check the validity of path
        """
        options = self.options
        task = options.all_tasks[self.rank]
        input_path = task['input_path']
        if self.worker.is_map_worker():
            output_path = task['output_path']
        else:
            output_path = os.path.dirname(task['output_path'])
        log_path = os.path.dirname(task['log_filebase'])
        tmp_path = task['tmp_dir']

        if options.mapreduce_force_mkdir:
            path_list = [output_path, log_path, tmp_path] 
            if self.worker.is_reduce_worker():
                path_list.append(input_path)
            for path in path_list:
                if not os.path.exists(path):
                    mkdir_cmd = 'mkdir -p %s' %path
                    self.run_cmd_and_wait(mkdir_cmd)
        else:
            if self.worker.is_map_worker():
                glob_cmd = 'ls %s 2>/dev/null' % input_path
                input_list = os.popen(glob_cmd).readlines()
                #input_list = glob.glob(input_path)
                if len(input_list) == 0:
                    raise RuntimeError('input_path %s does not match any file' 
                                       %input_path)
            else:
                if not os.path.exists(input_path):
                    raise RuntimeError('input_path %s does not exist' 
                                       %input_path)
            if output_path and not os.path.exists(output_path):
                raise RuntimeError('output_path %s does not exist' 
                                   %output_path)
            if log_path and not os.path.exists(log_path):
                raise RuntimeError('log path %s does not exist' %log_path)


#----------------------------------------------------------------------------#
# MODULE EPILOGUE
#----------------------------------------------------------------------------#

def _create_option_parser():
    """ Creates an option parser instance to handle command-line options.
    """
    usage = """%prog [options]
    communicator between workers and mrlite scheduler
    """
    parser = optparse.OptionParser(usage)
    parser.add_option('-r', '--rank', action='store', dest='rank', type='int',
                      help='rank of the worker ')
    parser.add_option('-s', '--server', action='store', dest='server',
                      help='ip address of server where scheduler is located')
    parser.add_option('-p', '--port', action='store', dest='port', type='int',
                      help='communiation port of server')
    parser.add_option('-d', '--dir', action='store', dest='dir',
                      help='tmp dir for worker')
    return parser


def main(argv):
    """ The main method for this module.
    """
    parser = _create_option_parser()
    (options, args) = parser.parse_args(argv)
    if None in [options.server, options.port, options.rank, options.dir]:
        mesg = '-s SERVER, -p PORT, -r RANK, -d DIR should not be empty'
        print(mesg)
        sys.exit(-1)

    try:
        comm = Communicator(options.server, options.port,
                            options.rank, options.dir)
        signal.signal(signal.SIGTERM, comm.sigterm_handler)
        start_time = time.time()
        comm.check_options()
        comm.run()
    except SystemExit:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        sys.exit(exc_value)
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        mesg = ''.join(traceback.format_exception(exc_type, exc_value,
                                                  exc_traceback))
        logging.info('cache exception in worker %s' %comm.worker.name)
        logging.info(mesg)
        comm.kill_job()
        comm.quit(-1)

#----------------------------------------------------------------------------#
if __name__ == '__main__':
    main(sys.argv[1:])
