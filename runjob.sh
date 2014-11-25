#!/bin/sh
hadoop fs -rm -r /output/timeline
hadoop fs -rm -r /output/normalized
hadoop fs -rm -r /output/variance_covariance
hadoop fs -rm -r /output/correlation
hadoop fs -rm -r /output/result

#StockNameIndexMap#DATA_FILE_PATH
hadoop fs -rm -r /stock/map/

hadoop jar ~/build-hadoop/jar/correlation/timeline.jar earth.japan.mrasu.hadoop.correlation.timeline.StockTimeLine /input/timeline /output/timeline
hadoop jar ~/build-hadoop/jar/correlation/timeline.jar earth.japan.mrasu.hadoop.correlation.stock_row_map.StockRowMapGenerator /output/timeline
hadoop jar ~/build-hadoop/jar/correlation/timeline.jar earth.japan.mrasu.hadoop.correlation.normalized_matrix.NormalizedMatrix /output/timeline /output/normalized
hadoop jar ~/build-hadoop/jar/correlation/timeline.jar earth.japan.mrasu.hadoop.correlation.variance_covariance_matrix.VarianceCovarianceMatrix /output/normalized /output/variance_covariance
hadoop jar ~/build-hadoop/jar/correlation/timeline.jar earth.japan.mrasu.hadoop.correlation.correlation.CorrelationMatrix /output/variance_covariance /output/correlation
hadoop jar ~/build-hadoop/jar/correlation/timeline.jar earth.japan.mrasu.hadoop.correlation.stock_row_map.StockRowMapRenovator /output/correlation /output/result
