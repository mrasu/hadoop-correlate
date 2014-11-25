package earth.japan.mrasu.hadoop.correlation.correlation;

import earth.japan.mrasu.hadoop.correlation.util.writable.Point;
import earth.japan.mrasu.hadoop.correlation.util.writable.PointValueWritable;
import earth.japan.mrasu.hadoop.correlation.util.writable.PointWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

/**
 * Created by mrasu on 2014/11/08.
 */
public class CorrelationReduce extends MapReduceBase implements Reducer<PointWritable, PointValueWritable, PointWritable, DoubleWritable>
{
    @Override
    public void reduce(PointWritable pointWritable, Iterator<PointValueWritable> values, OutputCollector<PointWritable, DoubleWritable> collector, Reporter reporter) throws IOException {
        Map<Integer, Double> diagonalValues = new HashMap<>();
        List<PointValueWritable> covariances = new ArrayList<>();

        while(values.hasNext()) {
            PointValueWritable pointValue = values.next();
            if(pointValue.getColumn() == pointValue.getRow()) {
                diagonalValues.put(pointValue.getColumn(), Math.sqrt(pointValue.getValue()));
            } else {
                //参照でコピーされると困るのでディープコピー。
                covariances.add(new PointValueWritable(false, pointValue.getRow(), pointValue.getColumn(), pointValue.getValue()));
            }
        }

        for(PointValueWritable covariance: covariances) {
            double rowDiagonalValue = diagonalValues.get(covariance.getRow());
            double columnDiagonalValue = diagonalValues.get(covariance.getColumn());

            double correlation = covariance.getValue() / (rowDiagonalValue * columnDiagonalValue);
            PointWritable point = new PointWritable(new Point(covariance.getRow(), covariance.getColumn()));
            collector.collect(point, new DoubleWritable(correlation));
        }

        int pos = pointWritable.getPoint().getX();
        collector.collect(new PointWritable(new Point(pos, pos)), new DoubleWritable(1));
    }
}
