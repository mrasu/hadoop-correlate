package earth.japan.mrasu.hadoop.correlation.variance_covariance_matrix;

import earth.japan.mrasu.hadoop.correlation.util.writable.PointValueWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by mrasu on 2014/11/03.
 */
public class MultiplicationReduce extends Reducer<Text, PointValueWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<PointValueWritable> values, Context context) throws InterruptedException, IOException {
        Map<Integer, Double> positionMap = new HashMap<>();
        for(PointValueWritable data : values) {
            Integer position;
            if(data.isTransition()) {
                position = data.getRow();
            } else {
                position = data.getColumn();
            }

            if(positionMap.containsKey(position)) {
                Double mapValue = positionMap.get(position);
                positionMap.put(position, mapValue * data.getValue());
            } else {
                positionMap.put(position, data.getValue());
            }
        }

        double sum = 0;
        for(Double pointValue: positionMap.values()) {
            sum += pointValue;
        }

        context.write(key, new DoubleWritable(sum));
    }
}
