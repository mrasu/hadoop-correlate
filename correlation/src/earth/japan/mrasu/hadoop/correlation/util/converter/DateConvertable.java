package earth.japan.mrasu.hadoop.correlation.util.converter;

import java.util.Date;

/**
 * Created by mrasu on 2014/11/03.
 */
public class DateConvertable extends Convertable<Date> {
    public DateConvertable(String text) {
        super(text);
    }

    public DateConvertable(Date date) {
        super(date);
    }

    @Override
    protected void constructFromString(String text) {
        Long milliSeconds = Long.parseLong(text);
        set(new Date(milliSeconds));
    }

    @Override
    protected String convertToString() {
        return Long.toString(get().getTime());
    }

}
