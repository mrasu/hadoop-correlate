package earth.japan.mrasu.hadoop.correlation.variance_covariance_matrix;

import earth.japan.mrasu.hadoop.correlation.util.StockNameIndexMap;
import earth.japan.mrasu.hadoop.correlation.util.writable.PointValueWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by mrasu on 2014/11/03.
 */
public class VarianceCovarianceMatrix {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance(new Configuration());
        job.setJobName(VarianceCovarianceMatrix.class.getName().toLowerCase());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MultiplicationElementMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PointValueWritable.class);

        job.setReducerClass(MultiplicationReduce.class);

        job.addCacheFile(new URI(StockNameIndexMap.DATA_FILE_PATH));

        KeyValueTextInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
