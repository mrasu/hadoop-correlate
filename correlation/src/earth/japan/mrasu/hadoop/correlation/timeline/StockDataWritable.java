package earth.japan.mrasu.hadoop.correlation.timeline;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * Created by mrasu on 2014/11/01.
 */

class StockDataWritable implements Writable {
    private Date date;
    private double price;

    public void set(Date date, double price) {
        this.date = date;
        this.price = price;
    }

    public Date getDate() {
        return date;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(date.getTime());
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        date = new Date(dataInput.readLong());
        price = dataInput.readDouble();
    }
}
