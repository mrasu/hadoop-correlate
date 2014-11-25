package earth.japan.mrasu.hadoop.correlation.normalized_matrix;

import earth.japan.mrasu.hadoop.correlation.util.writable.DoubleArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by mrasu on 2014/11/03.
 */
public class NormalizedMatrix {
    public static void main(String args[]) throws IOException {
        JobConf conf = new JobConf(NormalizedMatrix.class);
        conf.setJobName(NormalizedMatrix.class.getName());

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleArrayWritable.class);

        conf.setMapperClass(RowMap.class);

        conf.setReducerClass(NormalizedRowReduce.class);

        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
