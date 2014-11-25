package earth.japan.mrasu.hadoop.correlation.stock_row_map;

import earth.japan.mrasu.hadoop.correlation.util.StockNameIndexMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * Created by mrasu on 2014/11/09.
 */
public class StockRowMapRenovator {
    public static final String DELIMINATOR = ":::DEL::";

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance(new Configuration());
        job.setJobName(StockRowMapGenerator.class.getName().toLowerCase());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(StockMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(StockIndexReduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(StockNameIndexMap.DATA_FILE_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class StockMap extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text rows, Text text, Context context) throws IOException, InterruptedException {
            String stockName = rows.toString();
            context.write(new Text("1"), new Text(stockName + DELIMINATOR + text.toString()));
        }
    }

    static class StockIndexReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text num, Iterable<Text> rowTexts, Context context) throws IOException, InterruptedException {
            StockNameIndexMap nameMap = StockNameIndexMap.createFromCache(context, 0);
            //全て同一キーが渡されるので、1箇所でしか走らない。
            for(Text rowText: rowTexts) {
                String[] keyValue = rowText.toString().split(DELIMINATOR);
                String stockNames = keyValue[0];

                StringBuilder keyBuilder = new StringBuilder();
                for(String stockNumberString: stockNames.split(",")) {
                    keyBuilder.append(nameMap.getName(Integer.parseInt(stockNumberString)));
                    keyBuilder.append(",");
                }

                String key;
                if(keyBuilder.length() > 0) {
                    key = keyBuilder.substring(0, keyBuilder.length() - 1);
                } else {
                    key = "";
                }

                context.write(new Text(key), new Text(keyValue[1]));
            }
        }
    }
}
