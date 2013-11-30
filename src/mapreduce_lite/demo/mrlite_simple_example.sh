#!/bin/sh

# Find the path of this script, where testdata is located
script=`readlink -m $0`
script_path=`dirname $script`
testdata=$script_path/testdata

# Essentially, the scheduler can run any binary in specified 
# machines, so we use "/bin/echo" command to test scheduler

m1=10.1.149.174
m2=10.1.149.175

python ../scheduler/mrlite.py                  \
--mapreduce_cmd="/bin/echo -e Mapreduce-Lite " \
--mapreduce_map_io="{$m1}:WordCountMapper:recordio:$testdata/text-000?0-of-00002:/tmp/mrlite-$USER;
                    {$m2}:WordCountMapper:recordio:$testdata/text-000?1-of-00002:/tmp/mrlite-$USER;" \
--mapreduce_reduce_io="{$m1}:WordCountReducer:/tmp/mrlite-$USER:text:/tmp/mrlite-$USER"              \
--mapreduce_tmp_dir="{$m1,$m2}/tmp/mrlite-$USER"          \
--mapreduce_log_filebase="{$m1,$m2}/tmp/mrlite-$USER/log" \
--mapreduce_buffer_size=1024                              \
--mapreduce_ssh_port=36000                                \
--mapreduce_incremental_mode
