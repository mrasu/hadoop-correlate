package earth.japan.mrasu.hadoop.correlation.util.writable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mrasu on 2014/11/08.
 */
public class PointWritable implements WritableComparable<PointWritable> {
    private Point point;

    public PointWritable() {}

    public PointWritable(Point point) {
        this.point = point;
    }

    public void set(int x, int y) {
        point = new Point(x, y);
    }

    public Point getPoint() {
        return point;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(point.getX());
        dataOutput.writeInt(point.getY());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int x = dataInput.readInt();
        int y = dataInput.readInt();

        set(x, y);
    }

    @Override
    public String toString() {
        return point.getX() + "," + point.getY();
    }

    @Override
    public int compareTo(PointWritable other) {
        Point otherPoint = other.getPoint();
        if ((otherPoint.getX() > point.getX()) || (otherPoint.getY() > point.getY())) {
            return 1;
        } else if ((otherPoint.getX() == point.getX()) && (otherPoint.getY() == point.getY())) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public int hashCode() {
        return point.getX() + point.getY();
    }
}
