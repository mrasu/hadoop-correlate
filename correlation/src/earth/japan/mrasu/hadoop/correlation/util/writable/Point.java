package earth.japan.mrasu.hadoop.correlation.util.writable;

/**
 * Created by mrasu on 2014/11/08.
 */
public class Point {
    private int xPoint;
    private int yPoint;

    public Point(int x, int y) {
        if(0 > xPoint || 0 > yPoint) {
            throw new IllegalArgumentException();
        }
        xPoint = x;
        yPoint = y;
    }

    public boolean isDiagonal() {
        return xPoint == yPoint;
    }

    public int getMinDistance() {
        return Math.min(xPoint, yPoint);
    }

    public int getX() {
        return xPoint;
    }

    public int getY() {
        return yPoint;
    }
}
