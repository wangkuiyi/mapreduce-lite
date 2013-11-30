#!/bin/sh

# Find the path of this script, where testdata is located
script=`readlink -m $0`
script_path=`dirname $script`

# Mapreduce job is going to run in the following two machines, 
# please make sure that the test data is prepared in given 
# machines. 
#
# The output directory is set to "/tmp/mrlite-$USER", where 
# $USER is the bash enviroment variable storing the current 
# user name. If the current user is "mrlite", for instance, 
# then you can find the final output in "/tmp/mrlite-mrlite"

testdata=$script_path/testdata

m1=10.1.149.174
m2=10.1.149.175

python ../scheduler/mrlite.py   \
--mapreduce_cmd="mrl-wordcount" \
--mapreduce_map_io="{$m1}:WordCountMapper:text:$testdata/text-00000-of-00002:/tmp/mrlite-$USER;
                    {$m2}:WordCountMapperWithCombiner:text:$testdata/text-00001-of-00002:/tmp/mrlite-$USER;" \
--mapreduce_reduce_io="{$m1}:WordCountBatchReducer:/tmp/mrlite-$USER:text:/tmp/mrlite-$USER/output" \
--mapreduce_tmp_dir="{$m1,$m2}/tmp/mrlite-$USER"          \
--mapreduce_log_filebase="{$m1,$m2}/tmp/mrlite-$USER/log" \
--mapreduce_buffer_size=1024                              \
--mapreduce_ssh_port=36000                                \
--mapreduce_force_mkdir
