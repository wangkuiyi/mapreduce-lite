#!/bin/sh

# Find the path of this script, where testdata is located
script=`readlink -m $0`
script_path=`dirname $script`

# This script is used to test Mapreduce-Lite in current local 
# machine, modify the following ip address in variable "m1" 
# to your own local ip, then run this script.
#
# The output directory is set to "/tmp/mrlite-$USER", where 
# $USER is the bash enviroment variable storing the current 
# user name. If the current user is "mrlite", for instance, 
# then you can find the final output in "/tmp/mrlite-mrlite"

testdata=$script_path/testdata

m1=127.0.0.1

python ../scheduler/mrlite.py   \
--mapreduce_cmd="mrl-wordcount" \
--mapreduce_map_io="{$m1}:WordCountMapper:text:$testdata/text-00000-of-00002:/tmp/mrlite-mapper-$USER;
                    {$m1}:WordCountMapperWithCombiner:text:$testdata/text-00001-of-00002:/tmp/mrlite-mapper-$USER;" \
--mapreduce_reduce_io="{$m1}:WordCountBatchReducer:/tmp/mrlite-reducer-$USER:text:/tmp/mrlite-$USER/output" \
--mapreduce_tmp_dir="{$m1}/tmp/mrlite-$USER"          \
--mapreduce_log_filebase="{$m1}/tmp/mrlite-$USER/log" \
--mapreduce_buffer_size=1024                          \
--mapreduce_ssh_port=22                            \
--mapreduce_force_mkdir

