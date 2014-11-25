package earth.japan.mrasu.hadoop.correlation.timeline;

import earth.japan.mrasu.hadoop.correlation.util.converter.DateConvertable;
import earth.japan.mrasu.hadoop.correlation.util.converter.DoubleConvertable;
import earth.japan.mrasu.hadoop.correlation.util.converter.MapText;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by mrasu on 2014/11/01.
 */
public class StockDataToTimeLineReduce extends MapReduceBase implements Reducer<Text, StockDataWritable, Text, Text> {
    @Override
    public void reduce(Text stockName, Iterator<StockDataWritable> stockDataWritableIterator, OutputCollector<Text, Text> textMapWritableOutputCollector, Reporter reporter) throws IOException {
        Map<DateConvertable, DoubleConvertable> priceData = new HashMap<>();

        while(stockDataWritableIterator.hasNext()) {
            StockDataWritable data = stockDataWritableIterator.next();
            priceData.put(new DateConvertable(data.getDate()), new DoubleConvertable(data.getPrice()));
        }

        Text valueText = MapText.toText(priceData);
        textMapWritableOutputCollector.collect(new Text(stockName), new Text(valueText));
    }
}
