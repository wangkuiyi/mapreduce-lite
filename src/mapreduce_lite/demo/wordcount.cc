/*
This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
02110-1301, USA.
*/
// This is an example program that uses MapReduce-Lite, a lightweight
// MapReduce implementation.
//
// Usage:
//
// Suppose there are two computers in our cluster: vm1 and vm2.  Also
// suppose there are two chunks of input data: input-00000-of-00002 on
// vm1 at /home/yiwang/data, and input-00001-of-00002 on vm2 at
// /home/yiwang/data.  Note that it is important to keep the same
// directory structure on all computers, in this case, it is
// /home/yiwang/data.
//
// Our goal is to start two map workers: one on vm1 to process
// input-00000-of-00002, and another on vm2 to processing
// input-00001-of-00002; as well one reduce worker on vm1 to gether
// results into output-00000-of-00001.
//
// To achive above goal, we need to do the following things:
//
// 1. Build wordcount:
//      make -f Makefile.linux
//    This generates executable wordcount, which is statically linked with
//    all required libraries, including the GCC runtime library.
//
// 2. Put wordcount into computers and make Linux able to find them:
//     scp wordcount vm1:/home/yiwang/bin
//     scp wordcount vm2:/home/yiwang/bin
//    On each computer, add the following lines into /home/yiwang/.bashrc:
//     export PATH=/home/yiwang/bin:$PATH
//
// 3. Manually shard (put input files into their deserved locations):
//     scp input-00000-of-00002 vm1:/home/yiwang/data
//     scp input-00001-of-00002 vm2:/home/yiwang/data
//
// 4. List all computers we are going to use into a text file, say
//    ~/machine-list.  We list computers on which map workers will be
//    started, by the order of the input shard files, and then we list
//    computers on which reduce workers will be started, by the order
//    of the (planned) output shard files.  In this example,
//    ~/machine-list contains the following lines:
//      vm1
//      vm2
//      vm1
//
// 5. Ensure MPD has been started on all computers.  If not, issue the
//    following commands:
//      mpd --listenport=55555 & (on vm1)
//      mpd -h vm1 -p 55555 &    (on vm2 and all other nodes)
//
// 6. Issue the following command line.  Note that --num_map_workers +
//    --num_reducer_workers must be equal to -np, and the number of lines
//    of machine-list must be equal or larger than -np.
//
/*
  mpiexec -machinefile ~/machine-list -np 3 wordcount           \
  --mrml_num_map_workers=2                                      \
  --mrml_num_reduce_workers=1                                   \
  --mrml_input_format=text                                      \
  --mrml_output_format=text                                     \
  --mrml_input_filebase=/home/yiwang/data/input                 \
  --mrml_output_filebase=/home/yiwang/data/output               \
  --mrml_mapper_class=WordCountMapper                           \
  --mrml_reducer_class=WordCountReducer                         \
  --mrml_log_filebase=/tmp/$(date +%Y%m%d%H%M%S)-wordcount-log
*/
//
// *** Running on One-Computer Cluster ***
//
// In some cases (e.g., debugging), you might want to run all map
// workers and reduce workers on the same computer.  In this case,
// just no need to specify -machinefile.  An example is as follows:
/*
  mpiexec -np 3 ./wordcount                                     \
  --mrml_num_map_workers=2                                      \
  --mrml_num_reduce_workers=1                                   \
  --mrml_input_format=text                                      \
  --mrml_output_format=text                                     \
  --mrml_input_filebase=./testdata/input                        \
  --mrml_output_filebase=/tmp/wordcount-output                  \
  --mrml_mapper_class=WordCountMapper                           \
  --mrml_reducer_class=WordCountReducer                         \
  --mrml_log_filebase=/tmp/wordcount-log
 */
//
// *** Using Periodic-Flush ***
//
// The WordCountMapperWithCombiner can also be used to demonstrate the
// periodic-flush mechanism provided by MRML.  Just add a command line
// parameter to above command line:
/*
  --mrml_periodic_flush=2
 */
// The reduce output should be identical to that without using
// periodic-flush.
//
// *** Using Multi-pass Map ***
//
// This program can also be used to demonstrate multi-pass map, a
// unique feature of MRML.  Just add a command line parameter:
/*
  --mrml_multipass_map=2
 */
// In the reduce output, the word ocunts will be the multiplication of
// the number of passes.
//
// *** Using Batch Reduce ***
//
// In order to support reducing a large vocabulary of map outputs,
// MRML provides an batch reduce interface, which makes MRML looks
// identical to Google MapReduce.
/*
  mpiexec -np 3 ./wordcount                                     \
  --mrml_num_map_workers=2                                      \
  --mrml_num_reduce_workers=1                                   \
  --mrml_input_format=text                                      \
  --mrml_output_format=text                                     \
  --mrml_input_filebase=./testdata/input                        \
  --mrml_output_filebase=/tmp/wordcount-output                  \
  --mrml_mapper_class=MRWordCountMapper                         \
  --mrml_reducer_class=MRWordCountReducer                       \
  --mrml_log_filebase=/tmp/wordcount-log                        \
  --mrml_batch_reduction=true                                   \
  --mrml_disk_swap_file_dir=/tmp
 */

#include <string.h>

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <sstream>

#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include "src/base/common.h"
#include "src/mapreduce_lite/mapreduce_lite.h"
#include "src/sorted_buffer/sorted_buffer_iterator.h"
#include "src/strutil/split_string.h"

using namespace std;
using mapreduce_lite::Mapper;
using mapreduce_lite::IncrementalReducer;
using mapreduce_lite::BatchReducer;
using mapreduce_lite::ReduceInputIterator;


class WordCountMapper : public Mapper {
 public:
  void Map(const std::string& key, const std::string& value) {
    std::vector<std::string> words;
    SplitStringUsing(value, " ", &words);
    for (int i = 0; i < words.size(); ++i) {
      Output(words[i], "1");
    }
  }
};
REGISTER_MAPPER(WordCountMapper);


class WordCountMapperWithCombiner : public Mapper {
 public:
  void Map(const std::string& key, const std::string& value) {
    std::vector<std::string> words;
    SplitStringUsing(value, " ", &words);
    for (int i = 0; i < words.size(); ++i) {
      ++combined_results[words[i]];
    }
  }

  void Flush() {
    for (map<string, int>::const_iterator i = combined_results.begin();
         i != combined_results.end(); ++i) {
      ostringstream formater;
      formater << i->second;
      Output(i->first, formater.str());
    }
    // NOTE: for the correctness when we enable periodic-flush, we
    // must clear the intermediate data after flush it.
    combined_results.clear();
  }
 private:
  map<string, int> combined_results;
};
REGISTER_MAPPER(WordCountMapperWithCombiner);



class WordCountReducer : public IncrementalReducer {
 public:
  void Start() {
    Output("", "Following lines are reduce outputs:");
  }

  void* BeginReduce(const std::string& key, const std::string& value) {
    istringstream parser(value);
    int* r = new int;
    parser >> *r;
    return r;
  }

  void PartialReduce(const std::string& key,
                     const std::string& value,
                     void* partial_result) {
    istringstream parser(value);
    int count = 0;
    parser >> count;
    *static_cast<int*>(partial_result) += count;
  }

  void EndReduce(const string& key, void* partial_result) {
    int* p = static_cast<int*>(partial_result);
    ostringstream formater;
    formater << key << " " << *p; // If output format is Text, only value will
    Output(key, formater.str());  // be written.  So we write key into value.
    delete p;
  }

  void Flush() {
    Output("", "Above lines are reduce outputs.");
  }
};
REGISTER_INCREMENTAL_REDUCER(WordCountReducer);



// In batch reduction mode, reducers are derived from MR_Reducer and registered
// using REGISTER_MR_REDUCER; whereas in incremental reduction mode, reducers
// are derived from MRML_Reducer and registered using REGISTER_REDUCER.
class WordCountBatchReducer : public BatchReducer {
 public:
  void Start() {
    //Output("", "Following lines are MR reduce outputs:");
  }

  void Reduce(const string& key, ReduceInputIterator* values) {
    int sum = 0;
    LOG(INFO) << "key:[" << key << "]";
    for (; !values->Done(); values->Next()) {
      //LOG(INFO) << "value:[" << values->value() << "]";
      istringstream parser(values->value());
      int count = 0;
      parser >> count;
      sum += count;
    }
    ostringstream formater;
    formater << key << " " << sum;
    Output(key, formater.str());

    // Example of writing logs, choices are LOG(INFO), LOG(WARNING), LOG(FATAL)
    //LOG(INFO) << "Reduce for key : " << key;
  }

  void Flush() {
    //Output("", "Above lines are MR reduce outputs.");
  }
};
REGISTER_BATCH_REDUCER(WordCountBatchReducer);
