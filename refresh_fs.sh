#!/bin/bash
hdfs dfsadmin -safemode leave
stop-all.sh
rm -rf /tmp/hadoop*
hdfs namenode -format
start-all.sh
hdfs dfsadmin -safemode leave
