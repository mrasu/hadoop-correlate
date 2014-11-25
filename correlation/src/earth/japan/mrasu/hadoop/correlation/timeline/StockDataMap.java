package earth.japan.mrasu.hadoop.correlation.timeline;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by mrasu on 2014/11/01.
 */
public class StockDataMap extends MapReduceBase implements Mapper<Text, Text, Text, StockDataWritable> {
    @Override
    public void map(Text stockName, Text value, OutputCollector<Text, StockDataWritable> textStockDataWritableOutputCollector, Reporter reporter) throws IOException {
        String[] dateAndValue = value.toString().split("\t");

        //やってくる日付例: 2014-10-04 13:54:55.0
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
        try {
            Date priceDate = dateFormat.parse(dateAndValue[0]);
            double price = Double.parseDouble(dateAndValue[1]);

            StockDataWritable data = new StockDataWritable();
            data.set(priceDate, price);
            textStockDataWritableOutputCollector.collect(stockName, data);
        } catch (ParseException e) {
            //do nothing
        }
    }

}
