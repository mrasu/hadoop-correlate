#!/bin/sh
export CLASSPATH=$HADOOP_HOME/share/hadoop/common/hadoop-common-2.5.1.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.5.1.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:$HADOOP_HOME/share/hadoop/common/lib/hadoop-annotations-2.5.1.jar:/home/vagrant/build-hadoop/java_src/correlation/
javac ~/build-hadoop/java_src/correlation/earth/japan/mrasu/hadoop/correlation/timeline/StockTimeLine.java \
    ~/build-hadoop/java_src/correlation/earth/japan/mrasu/hadoop/correlation/normalized_matrix/NormalizedMatrix.java \
    ~/build-hadoop/java_src/correlation/earth/japan/mrasu/hadoop/correlation/variance_covariance_matrix/VarianceCovarianceMatrix.java \
    ~/build-hadoop/java_src/correlation/earth/japan/mrasu/hadoop/correlation/correlation/CorrelationMatrix.java \
    ~/build-hadoop/java_src/correlation/earth/japan/mrasu/hadoop/correlation/stock_row_map/StockRowMapGenerator.java \
    ~/build-hadoop/java_src/correlation/earth/japan/mrasu/hadoop/correlation/stock_row_map/StockRowMapRenovator.java \
    -d ~/build-hadoop/classes/correlation/
jar -cvf ~/build-hadoop/jar/correlation/timeline.jar -C ~/build-hadoop/classes/correlation/ .
#source ~/build-hadoop/runjob.sh