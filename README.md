MapReduce Lite is a C++ implementation of the MapReduce programming paradigm.

## Pros

First of all, MapReduce Lite is Lite!

  * It does not rely on a distributed filesystem -- it can simply use local filesystem;
  * It does not have a dynamic task scheduling system -- the map/reduce tasks were scheduled before the parallel job is started;
  * There is zero deployment / configuration cost -- just link your program against the MapReduce Lite library statically and run it.

In addition to the functions described in Google's famous MapReduce paper, known as batch reduction mode in MapReduce Lite, there is also an incremental reduction mode, doing the shuffling phase in memory and without disk access. In this mode, MapReduce Lite programs run much faster than rigid implementations like Hadoop.

## Cons

As a lite implementation, MapReduce Lite does not support fault recovery, which, however, is arguably not too difficult to achieve if we do not require backup workers or global counters and can use a distributed filesystem (DFS).

## Applications

In Tencent, we have been using MapReduce Lite with a Tencent's DFS to run jobs like search engine log processing, search and ads click model training, and distributed language model training.

## A Sample

    using mapreduce_lite::Mapper;
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

    class WordCountBatchReducer : public BatchReducer {
     public:
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
      }
    };
    REGISTER_BATCH_REDUCER(WordCountBatchReducer);


## Updates

  1. 2013-10-4: MapReduce Lite supports Mac OS X and FreeBSD in addition to Linux. You can build your MapReduce Lite programs using GCC or Clang.
