FROM flink:1.19.0-scala_2.12-java11

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev openjdk-11-jdk && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink from the pinned requirements (single source of truth)
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt
