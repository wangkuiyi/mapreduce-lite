# Build MapReduce Lite

MapReduce Lite can be built on Linux, Mac OS X and FreeBSD, with GCC
(>=4.5.1) or Clang (>=3.0).

MapReduce Lite depends on the following tools or libraries:

  * [CMake](http://www.cmake.org) is used to manage and build the
    project.
  * [Google Protocol Buffers](http://code.google.com/p/protobuf/) is
    used for inter-worker communication and disk storage.
  * [GFlags](http://code.google.com/p/google-gflags/) is used to parse
    command line options.
  * [Google Test](http://code.google.com/p/googletest/) is used to
    write and run unit tests.
  * [Boost](http://www.boost.org/) helps developing multi-threading in
    asynchronous inter-worker communication and provides
    cross-platform filesystem support.
  * [libevent](http://libevent.org) is used to support efficient
    asynchronous network communication.

MapReduce Lite used to rely on the Linux-specific system call,
[epoll](http://linux.die.net/man/4/epoll).  Hao Yan changed the
situation in 2013-10-03 by replacing calls to epoll by those to
`libevent` in
[this issue](http://code.google.com/p/mapreduce-lite/issues/detail?id=2).

# Install Building Tools

## Install GCC or Clang

MapReduce Lite can be built using GCC or Clang.

  * On Cygwin, run `setup.exe` and install `gcc` and `binutils`.
  * On Debian/Ubuntu Linux, type the command `sudo apt-get install gcc binutils` to install GCC, or `sudo apt-get install clang` to install Clang.
  * On FreeBSD, type the command `sudo pkg_add -r clang` to install Clang.  Note that since version 9.0, FreeBSD does not update GCC but relies completely on Clang.
  * On Mac OS X, install XCode gets you Clang.

## Install CMake

MapReduce Lite need CMake with version >= 2.8.0 to compile Google Protocol Buffer definitions.

To install CMake from binary packages:

  * On Cygwin, run `setup.exe` and install `cmake`.
  * On Debian/Ubuntu Linux, type the command `sudo apt-get install cmake`.
  * On FreeBSD, type the command `sudo pkg_add -r cmake`.
  * On Mac OS X, if you have [http://mxcl.github.com/homebrew/ Howebew], you can use the command `brew install cmake`, or if you have [http://www.macports.org/ MacPorts], run `sudo port install cmake`.  You won't want to have both Homebrew and !MacPorts installed.

You can also download binary or source package of [CMake](http://www.cmake.org/cmake/resources/software.html) and install it manually.

## Install Protobuf

MapReduce Lite requires protobuf with version >= 2.3.0.

To install protobuf from binary packages:

  * On Debian/Ubuntu Linux, you can run `sudo apt-get install libprotobuf-dev libprotoc-dev`.
  * On FreeBSD, you can run `sudo pkg_add -r protobuf`.
  * On Mac OS X, you can run `brew install protobuf protobuf-c`.

Or, you can install protobuf from source code:

  1. Download source code package, say `protobuf-2.5.0.tar.bz2`, from http://code.google.com/p/protobuf
  1. You need to install protobuf into a standard place, e.g., `/usr/local/`, so that CMake can find the protoc compiler and the library:

        tar xjf protobuf-2.5.0.tar.bz2
        cd protobuf-2.5.0
        ./configure --disable-shared --enable-static
        make
        sudo make install

# Install Dependencies

If you want to build and run MapReduce Lite on a single node, you can simply install dependent packages using the package manangement tool on your system.  For example, on MacOS X, you can use Homebrew:

    brew install gflags boost libevent
    
Or, on FreeBSD,

    pkg_add -r gflags boost libevent    

However, if you want to use MapReduce on a cluster of computers, you might want to build dependents manually from source code.  You can build only static dependent libraries, and build MapReduce Lite programs linking to these dependents statically.  This saves you from being bothered by deploying a set of shared libraries and concerning their versions.  The following sections shows how to build dependent packages from source code.

## Install GFlags

  1. Download the source code package (e.g., `gflags-2.0.tar.gz`) from http://code.google.com/p/google-gflags
  1. Unpack the source code anywhere (e.g., `./gflags-2.0`)
  1. You are free to install gflags anywhere (e.g., `/home/you/3rd-party/gflags`):

        cd gflags-2.0
        ./configure --prefix=/home/you/3rd-party/gflags-2.0 # if you want to use default build tool, or
        # CC=clang CXX=clang++ ./configure --prefix=/home/you/3rd-party/gflags-2.0
        make && make install
        ln -s /home/you/3rd-party/gflags-2.0 /home/you/3rd-party/gflags

## Install Boost

  1. Download source code of [Boost](http://www.boost.org/users/download/).
  1. Unpack, build and install:

        cd /home/you/3rd-party/
        tar xjf boost_1_54_0.tar.bz2
        cd boost_1_54_0
        ./bootstrap --prefix=/home/you/3rd-party/boost-1_54_0
        ./b2 -j8 # if you want to use default build tool, or
        # ./b2 -j8 toolset=clang
        ./b2 install
        ln -s /home/you/3rd-party/boost-1_54_0 /home/you/3rd-party/boost
        
  NOTE: We use a library named "libboost_thread-mt" in the file "src/mapreudce-lite/CMakeLists.txt",  but on some systems such as ubuntu and Mac OSX,
  using './b2 -j8' will not generate this library, instead, it will generate the "libboost_thread" by default. If your Linker    shows the error:
  can not find libboost_thread-mt, you can just modify the file "src/mapreduce-lit/CMakeLists.txt" to use 'boost_thread'. 

## Install libevent

  1. Download source code of [libevent](http://libevent.org).
  1. Unpack, build and install:

        cd /home/you/3rd-party
        tar xjf libevent-2.0.21-stable.tar.bz2
        cd libevent-2.0.21-stable
        ./configure --prefix=/home/you/3rd-party/libevent-2.0.21-stable # or
        # CC=clang CXX=clang++ ./configure --prefix=/home/you/3rd-party/libevent-2.0.21-stable
        make && make install
        ln -s /home/you/3rd-party/libevent-2.0.21-stable /home/wyi/3rd-party/libevent

# Build MapReduce Lite

With above dependencies installed, building MapReduce Lite is easy:

  1. Checkout the code:

        git clone https://github.com/wangkuiyi/mapreduce-lite

  1. If you build all above thirdparty libraries from source code, you need to tell cmake whether you have installed them.  Open `mapreduce-lite-read-only/CMakeLists.txt` and replace the value in the following line by the directory where you put third-party libraries.

        set(THIRD_PARTY_DIR "/home/you/3rd-party")

  1. Replace the value in the following line by the directory where you want to install MapReduce Lite and demos.

        set(CMAKE_INSTALL_PREFIX "/home/you/mapreduce-lite")

  1. Install GoogleTest 

     MapReduce Lite uses [googletest](http://code.google.com/p/googletest/) framework for unit testing.  Since version 1.6.0, it is no longer recommended to install googletest system-wide; instead, we need to download the source code and incorporate it with MapReduce Lite source code.
     
     1. Download the source code package (e.g., `gtest-1.7.0.tar.bz2`).
     1. Unpack the source code, say, to `/home/you/3rd-party/gtest-1.7.0`
     1. Make a symbolic link.  You do not need to build googletest; it will be built as part of MapReduce Lite.
 
            cd mapreduce-lite/src
            ln -s where/you/unpack/gtest gtest


  1. Build MapReduce Lite

        mkdir /tmp/mapreduce-lite
        cd /tmp/mapreduce-lite
        cmake ../mapreduce-lite-read-only # if you want to use default build tools, or
        # CXX=clang++ CC=clang cmake ../mapreduce-lite-read-only
        # CXX=g++     CC=gcc   cmake ../mapreduce-lite-read-only
        make -j8
        make install
