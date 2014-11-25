package earth.japan.mrasu.hadoop.correlation.variance_covariance_matrix;

import earth.japan.mrasu.hadoop.correlation.util.writable.DoubleArrayWritable;
import earth.japan.mrasu.hadoop.correlation.util.writable.PointValueWritable;
import earth.japan.mrasu.hadoop.correlation.util.StockNameIndexMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by mrasu on 2014/11/03.
 */
public class MultiplicationElementMap extends Mapper<Text, Text, Text, PointValueWritable> {
    @Override
    public void map(Text stockName, Text input, Context context) throws IOException, InterruptedException {
        StockNameIndexMap stockMap = StockNameIndexMap.createFromCache(context, 0);
        int stockRowIndex = stockMap.getRow(stockName.toString());

        DoubleArrayWritable doubleArrayWritable = DoubleArrayWritable.createFromText(input);
        List<Double> values = doubleArrayWritable.getValues();

        int columnIndex = 0;

        for(Double value : values) {
            for(int multiplicationUsingColumnIndex = 0; multiplicationUsingColumnIndex < stockMap.getMaxRow(); multiplicationUsingColumnIndex++) {
                PointValueWritable data = new PointValueWritable(false, stockRowIndex, columnIndex, value);
                String key = stockRowIndex + "," + multiplicationUsingColumnIndex;
                context.write(new Text(key), data);
            }

            for(int transitionRowIndex = 0; transitionRowIndex < stockMap.getMaxRow(); transitionRowIndex++) {
                PointValueWritable data = new PointValueWritable(true, stockRowIndex, columnIndex, value);
                String transitionKey = transitionRowIndex + "," + stockRowIndex;
                context.write(new Text(transitionKey), data);
            }
            columnIndex++;
        }
    }
}
