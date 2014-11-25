package earth.japan.mrasu.hadoop.correlation.util.converter;

/**
 * Created by mrasu on 2014/11/03.
 */
public class DoubleConvertable extends Convertable<Double>{
    public DoubleConvertable(String text) {
        super(text);
    }

    public DoubleConvertable(Double value) {
        super(value);
    }

    @Override
    protected void constructFromString(String text) {
        set(Double.parseDouble(text));
    }

    @Override
    protected String convertToString() {
        return Double.toString(get());
    }
}
