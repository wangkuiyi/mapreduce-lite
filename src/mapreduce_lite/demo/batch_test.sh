echo "start map worker 0"
./mrl-wordcount \
  --mr_input_filepattern=./testdata/text-00000-of-00002       \
  --mr_reduce_input_filebase=./testdata/tmp/mapoutput         \
  --mr_reduce_input_buffer_size=64                            \
  --mr_log_filebase=./testdata/tmp/log                        \
  --mr_num_map_workers=2                                      \
  --mr_reduce_workers="m1:7070,m2:7070,m2:7071"               \
  --mr_map_worker_id=0                                        \
  --mr_mapper_class=WordCountMapper                           \
  --mr_input_format=text                                      \
  --mr_batch_reduction=true

echo "start map worker 1"
./mrl-wordcount \
  --mr_input_filepattern=./testdata/text-00001-of-00002       \
  --mr_reduce_input_filebase=./testdata/tmp/mapoutput         \
  --mr_reduce_input_buffer_size=64                            \
  --mr_log_filebase=./testdata/tmp/log                        \
  --mr_num_map_workers=2                                      \
  --mr_reduce_workers="m1:7070,m2:7070,m2:7071"               \
  --mr_map_worker_id=1                                        \
  --mr_mapper_class=WordCountMapper                           \
  --mr_input_format=text                                      \
  --mr_batch_reduction=true

echo "move map_output to reduce_input"
ls testdata/tmp/mapoutput-mapper-0000* | awk '{ cmd = sprintf("mv "$1" testdata/tmp/reduce-input-%010d", NR-1); print cmd; system(cmd);} '

echo "start reduce worker 0"
./mrl-wordcount \
  --mr_output_files=./testdata/tmp/output                     \
  --mr_reduce_input_filebase=./testdata/tmp/reduce-input      \
  --mr_reduce_input_buffer_size=64                            \
  --mr_num_reduce_input_buffer_files=6                        \
  --mr_log_filebase=./testdata/tmp/log                        \
  --mr_num_map_workers=2                                      \
  --mr_reduce_workers="m1:7070,m2:7070,m2:7071"               \
  --mr_reduce_worker_id=0                                     \
  --mr_reducer_class=WordCountBatchReducer                    \
  --mr_output_format=text                                     \
  --mr_batch_reduction=true

