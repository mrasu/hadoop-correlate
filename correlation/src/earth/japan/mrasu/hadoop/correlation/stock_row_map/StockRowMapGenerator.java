package earth.japan.mrasu.hadoop.correlation.stock_row_map;

import earth.japan.mrasu.hadoop.correlation.util.StockNameIndexMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by mrasu on 2014/11/09.
 */
public class StockRowMapGenerator {
    public static void main(String args[]) throws IOException {
        JobConf conf = new JobConf(StockRowMapGenerator.class);
        conf.setJobName(StockRowMapGenerator.class.getName().toLowerCase());

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(StockMap.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setReducerClass(StockIndexReduce.class);

        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));

        Path outputPath = new Path(StockNameIndexMap.DATA_FILE_PATH);
        FileOutputFormat.setOutputPath(conf, outputPath);

        //hadoop fs -rm -r
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(outputPath, true);

        JobClient.runJob(conf);
    }

    static class StockMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text stockNameText, Text text, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
            String stockName = stockNameText.toString();
            collector.collect(new Text("1"), new Text(stockName));
        }
    }

    static class StockIndexReduce extends MapReduceBase implements Reducer<Text, Text, IntWritable, Text> {
        @Override
        public void reduce(Text text, Iterator<Text> stocks, OutputCollector<IntWritable, Text> collector, Reporter reporter) throws IOException {
            //全て同一キーが渡されるので、1箇所でしか走らない。
            int i = 0;
            while(stocks.hasNext()) {
                String stockName = stocks.next().toString();
                collector.collect(new IntWritable(i), new Text(stockName));
                i++;
            }
        }
    }
}
