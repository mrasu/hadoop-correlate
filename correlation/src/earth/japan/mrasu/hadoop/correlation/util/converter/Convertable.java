package earth.japan.mrasu.hadoop.correlation.util.converter;

/**
 * Created by mrasu on 2014/11/03.
 */
public abstract class Convertable<T> {
    private T value;

    public Convertable(String text) {
        constructFromString(text);
    }

    public Convertable(T value) {
        set(value);
    }

    protected abstract void constructFromString(String text);

    protected abstract String convertToString();

    public T get() {
        return value;
    }

    public void set(T t) {
        value = t;
    }
}
