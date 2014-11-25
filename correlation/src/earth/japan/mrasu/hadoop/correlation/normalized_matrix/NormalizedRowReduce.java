package earth.japan.mrasu.hadoop.correlation.normalized_matrix;

import earth.japan.mrasu.hadoop.correlation.util.writable.DoubleArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by mrasu on 2014/11/03.
 */
public class NormalizedRowReduce extends MapReduceBase implements Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {
    @Override
    public void reduce(Text stockName, Iterator<DoubleArrayWritable> pricesWritables, OutputCollector<Text, DoubleArrayWritable> collector, Reporter reporter) throws IOException {
        boolean isFirst = true;
        DoubleArrayWritable data = null;
        while(pricesWritables.hasNext()) {
            if(isFirst == false) {
                throw new IllegalStateException("keyが同じものが２箇所に存在する。タイムライン化で1銘柄1列になっているはずなのでデータが不正。");
            }
            data = pricesWritables.next();
            isFirst = false;
        }

        List<Double> prices = data.getValues();
        List<Double> normalizedPrices = normalize(prices);

        collector.collect(stockName, new DoubleArrayWritable(normalizedPrices.toArray(new Double[normalizedPrices.size()])));
    }

    private List<Double> normalize(List<Double> prices) {
        //ボクシングが重なっているので遅い可能性がある、
        double average = getAverage(prices);

        List<Double> normalizedPriceList = new ArrayList<>();
        for(Double price : prices) {
            normalizedPriceList.add(price - average);
        }
        return normalizedPriceList;
    }
    private double getAverage(List<Double> prices) {
        double sum = 0;
        for(double price : prices) {
            sum += price;
        }

        return sum / prices.size();
    }

}
