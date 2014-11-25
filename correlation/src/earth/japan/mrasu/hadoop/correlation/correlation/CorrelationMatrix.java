package earth.japan.mrasu.hadoop.correlation.correlation;

import earth.japan.mrasu.hadoop.correlation.util.writable.PointValueWritable;
import earth.japan.mrasu.hadoop.correlation.util.writable.PointWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by mrasu on 2014/11/08.
 */
public class CorrelationMatrix {
    public static void main(String args[]) throws IOException {
        JobConf conf = new JobConf(CorrelationMatrix.class);
        conf.setJobName(CorrelationMatrix.class.getName().toLowerCase());

        conf.setOutputKeyClass(PointWritable.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(CovarianceCollectMap.class);
        conf.setMapOutputKeyClass(PointWritable.class);
        conf.setMapOutputValueClass(PointValueWritable.class);

        conf.setReducerClass(CorrelationReduce.class);

        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
