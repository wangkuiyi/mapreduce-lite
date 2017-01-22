FROM ubuntu:16.04


RUN echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial main restricted universe multiverse" > /etc/apt/sources.list && \
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-backports main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ xenial-security main restricted universe multiverse" >> /etc/apt/sources.list

RUN apt-get update

RUN apt-get install -y binutils cmake libprotobuf-dev libprotoc-dev libgflags-dev libboost-dev libevent-dev && \
	apt-get install -y make protobuf-compiler build-essential libboost-program-options-dev libboost-regex-dev libboost-filesystem-dev libboost-system-dev libboost-thread-dev && \
    apt-get install -y openssh-server python

RUN ssh-keygen -b 2048 -t rsa -f ~/.ssh/id_rsa  -N "" && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN /etc/init.d/ssh start && ssh-keyscan -H localhost >> ~/.ssh/known_hosts && ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts

ADD . /root/mapreduce-lite

WORKDIR /root/mapreduce-lite
# RUN wget https://github.com/google/googletest/archive/release-1.8.0.tar.gz && \
# 	tar xzvf release-1.8.0.tar.gz && \
# 	mv googletest-release-1.8.0/ src/gtest

RUN cmake . && make -j8 && make install

WORKDIR /opt/mapreduce-lite


CMD /etc/init.d/ssh start && cd /opt/mapreduce-lite && bash