package earth.japan.mrasu.hadoop.correlation.util.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mrasu on 2014/11/03.
 */
public class PointValueWritable implements Writable {
    private boolean isTransition;
    private int row;
    private int column;
    private double value;

    public PointValueWritable() {}
    public PointValueWritable(boolean isTransition, int row, int column, double value) {
        this.isTransition = isTransition;
        this.row = row;
        this.column = column;
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(row);
        dataOutput.writeInt(column);
        dataOutput.writeDouble(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        row = dataInput.readInt();
        column = dataInput.readInt();
        value = dataInput.readDouble();
    }

    public boolean isTransition() {
        return isTransition;
    }

    public int getRow() {
        return row;
    }

    public int getColumn() {
        return column;
    }

    public double getValue() {
        return value;
    }
}
