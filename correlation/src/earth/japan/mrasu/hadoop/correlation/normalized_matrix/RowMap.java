package earth.japan.mrasu.hadoop.correlation.normalized_matrix;

import earth.japan.mrasu.hadoop.correlation.util.converter.DateConvertable;
import earth.japan.mrasu.hadoop.correlation.util.writable.DoubleArrayWritable;
import earth.japan.mrasu.hadoop.correlation.util.converter.DoubleConvertable;
import earth.japan.mrasu.hadoop.correlation.util.converter.MapText;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

/**
 * Created by mrasu on 2014/11/03.
 */
public class RowMap extends MapReduceBase implements Mapper<Text, Text, Text, DoubleArrayWritable> {
    @Override
    public void map(Text stockName, Text value, OutputCollector<Text, DoubleArrayWritable> collector, Reporter reporter) throws IOException {
        Map<DateConvertable, DoubleConvertable> priceData = MapText.toMap(value, DateConvertable.class, DoubleConvertable.class);

        TreeMap<Date, Double> sortedData = new TreeMap<>();
        for(Map.Entry<DateConvertable, DoubleConvertable> entry : priceData.entrySet()) {
            sortedData.put(entry.getKey().get(), entry.getValue().get());
        }

        collector.collect(stockName,  new DoubleArrayWritable(sortedData.values().toArray(new Double[sortedData.size()])));
    }
}
