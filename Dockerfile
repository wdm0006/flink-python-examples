FROM flink:latest

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev openjdk-11-jdk && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
RUN pip3 install numpy apache-flink  