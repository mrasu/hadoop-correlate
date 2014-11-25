package earth.japan.mrasu.hadoop.correlation.correlation;

import earth.japan.mrasu.hadoop.correlation.util.writable.Point;
import earth.japan.mrasu.hadoop.correlation.util.writable.PointValueWritable;
import earth.japan.mrasu.hadoop.correlation.util.writable.PointWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by mrasu on 2014/11/08.
 */
public class CovarianceCollectMap extends MapReduceBase implements Mapper<Text, Text, PointWritable, PointValueWritable> {
    @Override
    public void map(Text pointText, Text valueText, OutputCollector<PointWritable, PointValueWritable> collector, Reporter reporter) throws IOException {
        String[] pointArrayText = pointText.toString().split(",");
        int x = Integer.parseInt(pointArrayText[0]);
        int y = Integer.parseInt(pointArrayText[1]);
        double value = Double.parseDouble(valueText.toString());

        Point point = new Point(x, y);
        int minDistance = point.getMinDistance();

        if(point.isDiagonal()) {
            for(int i = 0; i < minDistance + 1; i++) {
                PointWritable gatheringPoint = new PointWritable(new Point(i, i));
                collector.collect(
                    gatheringPoint,
                    new PointValueWritable(false, point.getX(), point.getY(), value)
                );
            }
        } else {
            PointWritable gatheringPoint = new PointWritable(new Point(minDistance, minDistance));
            collector.collect(
                gatheringPoint,
                new PointValueWritable(false, point.getX(), point.getY(), value)
            );
        }
    }
}
