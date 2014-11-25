package earth.japan.mrasu.hadoop.correlation.util.writable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mrasu on 2014/11/03.
 */
public class DoubleArrayWritable extends ArrayWritable{
    private static final String SEPARATOR = ",";

    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(Double[] doubles) {
        super(DoubleWritable.class);

        DoubleWritable[] writables = new DoubleWritable[doubles.length];
        for(int i = 0; i < writables.length; i++) {
            writables[i] = new DoubleWritable(doubles[i]);
        }
        set(writables);
    }

    public List<Double> getValues() {
        List<Double> values = new ArrayList<>();
        for(Writable writable : get())
        {
            DoubleWritable doubleWritable = (DoubleWritable)writable;
            values.add(doubleWritable.get());
        }
        return values;
    }

    @Override
    public String toString() {
        DoubleWritable[] values = (DoubleWritable[])get();
        StringBuilder sb = new StringBuilder();
        for(String s: super.toStrings()) {
            sb.append(s).append(SEPARATOR);
        }
        return sb.toString();
    }

    public static DoubleArrayWritable createFromText(Text text) {
        String[] stringValues = text.toString().split(SEPARATOR);
        Double[] values = new Double[stringValues.length];

        for(int i = 0; i < values.length; i++) {
            values[i] = Double.parseDouble(stringValues[i]);
        }

        return new DoubleArrayWritable(values);
    }
}
