echo "start reduce worker 0"
./mrl-wordcount \
  --mr_output_files=./testdata/tmp/output                     \
  --mr_num_reduce_input_buffer_files=6                        \
  --mr_log_filebase=./testdata/tmp/log                        \
  --mr_num_map_workers=2                                      \
  --mr_reduce_workers="127.0.0.1:12345"                       \
  --mr_reduce_worker_id=0                                     \
  --mr_reducer_class=WordCountReducer                         \
  --mr_output_format=text                                     \
  --mr_batch_reduction=false &

echo "sleep 3"
sleep 3

echo "start map worker 0"
./mrl-wordcount \
  --mr_input_filepattern=./testdata/text-00000-of-00002       \
  --mr_log_filebase=./testdata/tmp/log                        \
  --mr_num_map_workers=2                                      \
  --mr_reduce_workers="127.0.0.1:12345"                       \
  --mr_map_worker_id=0                                        \
  --mr_mapper_class=WordCountMapper                           \
  --mr_input_format=text                                      \
  --mr_batch_reduction=false &

echo "start map worker 1"
./mrl-wordcount \
  --mr_input_filepattern=./testdata/text-00001-of-00002       \
  --mr_log_filebase=./testdata/tmp/log                        \
  --mr_num_map_workers=2                                      \
  --mr_reduce_workers="127.0.0.1:12345"                       \
  --mr_map_worker_id=1                                        \
  --mr_mapper_class=WordCountMapper                           \
  --mr_input_format=text                                      \
  --mr_batch_reduction=false &

wait

echo "done"

