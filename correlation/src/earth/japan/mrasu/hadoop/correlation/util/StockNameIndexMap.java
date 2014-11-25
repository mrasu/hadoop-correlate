package earth.japan.mrasu.hadoop.correlation.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mrasu on 2014/11/04.
 */
public class StockNameIndexMap {
    public static final String DATA_FILE_PATH = "/stock/map/";

    public Map<String, Integer> nameMap;
    public Map<Integer, String> numberMap;

    public StockNameIndexMap(Map<String, Integer> nameMap) {
        this.nameMap = nameMap;
        numberMap = reverseMap();
    }

    public static StockNameIndexMap createFromCache(Reducer.Context context, int cacheFileIndex) {
        try {
            Path path = new Path(context.getCacheFiles()[cacheFileIndex]);
            return createFromPath(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static StockNameIndexMap createFromCache(Mapper.Context context, int cacheFileIndex) {
        try {
            Path path = new Path(context.getCacheFiles()[cacheFileIndex]);
            return createFromPath(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static StockNameIndexMap createFromPath(Path path) {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(path);

            Map<String, Integer> map = new HashMap<>();

            for (FileStatus fileStatus : status) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));

                String line;
                while ((line = br.readLine()) != null) {
                    String[] stock = line.split("\t", 2);
                    int number = Integer.parseInt(stock[0]);
                    String name = stock[1];

                    map.put(name, number);
                }
            }
            return new StockNameIndexMap(map);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public Map<Integer, String> reverseMap() {
        Map<Integer, String> map = new HashMap<>();
        for(Map.Entry<String, Integer> entry: nameMap.entrySet()) {
            map.put(entry.getValue(), entry.getKey());
        }
        return map;
    }

    public int getRow(String name) {
        return nameMap.get(name);
    }

    public String getName(int number) {
        return numberMap.get(number);
    }
    public int getMaxRow() {
        return nameMap.size();
    }
}
