package earth.japan.mrasu.hadoop.correlation.timeline;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by mrasu on 2014/11/01.
 */
public class StockTimeLine {
    public static void main(String args[]) throws IOException {
        JobConf conf = new JobConf(StockTimeLine.class);
        conf.setJobName(StockTimeLine.class.getName().toLowerCase());

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class);

        conf.setMapperClass(StockDataMap.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(StockDataWritable.class);

        //conf.setCombinerClass(StockDataToTimeLineReduce.class);
        conf.setReducerClass(StockDataToTimeLineReduce.class);

        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
