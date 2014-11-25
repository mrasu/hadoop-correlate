package earth.japan.mrasu.hadoop.correlation.util.converter;

import org.apache.hadoop.io.Text;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Created by mrasu on 2014/11/03.
 *
 * Mapとhadoop.io.Textを入れ替える。
 * エスケープなどはないので、即座につぶれる可能性がある。
 *
 * 高階関数がJava7では使えないので、toMapが汚い。
 */
public class MapText {
    private static final String KEY_VALUE_SEPARATOR = ":";
    private static final String ITEMS_SEPARATOR = ",";

    public static <K extends Convertable, V extends Convertable> Text toText(Map<K, V> map) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<K, V> entry: map.entrySet())
        {
            sb.append(entry.getKey().convertToString());
            sb.append(KEY_VALUE_SEPARATOR);
            sb.append(entry.getValue().convertToString());
            sb.append(ITEMS_SEPARATOR);
        }

        return new Text(sb.toString());
    }

    public static <K extends Convertable, V extends Convertable> Map<K, V> toMap(Text text, Class<K> keyClass, Class<V> valueClass){
        List<String[]> pairs = getPairs(text.toString());

        Constructor<K> keyConstructor = getConvertableConstructor(keyClass);
        Constructor<V> valueConstructor = getConvertableConstructor(valueClass);

        int mapSize = (int)(pairs.size() * 1.3);
        Map<K, V> resultMap = new HashMap<>(mapSize);
        for(String[] pair : pairs) {
            K key = getInstance(keyConstructor, pair[0]);
            V value = getInstance(valueConstructor, pair[1]);
            resultMap.put(key, value);
        }

        return resultMap;
    }

    private static <K extends Convertable> Constructor<K> getConvertableConstructor(Class<K> clazz) {
        try {
            return clazz.getConstructor(String.class);
        } catch (NoSuchMethodException e) {
            //Convertableがstring引数のコンストラクタを持っているので、ここには絶対に来ない。
            //もっと良い書き方があればそちらを採用したい。
            throw new IllegalStateException("ConvertableにStringを引数とするコンストラクタがありません。", e);
        }
    }

    private static <K extends  Convertable> K getInstance(Constructor<K> constructor, String argument) {
        try {
            return constructor.newInstance(argument);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            //Convertableがstring引数のコンストラクタを持っているので、ここには絶対に来ない。
            //もっと良い書き方があればそちらを採用したい。
            throw new IllegalStateException("ConvertableにStringを引数とするコンストラクタがありません。", e);
        }
    }

    private static List<String[]> getPairs(String text) {
        List<String[]> pairs = new ArrayList<>();

        String[] contents = text.split(ITEMS_SEPARATOR);
        for(String keyValue : contents) {
            String[] pair = keyValue.split(KEY_VALUE_SEPARATOR, 2);
            pairs.add(pair);
        }

        return pairs;
    }
}
