/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 *
 * @author joseluis
 */
public class Utils {

    /**
     * This method converts meters per second to miles per hour.
     */
    /*public static int getMilesPerHour(int metersPerSecond) {
        return (int) Math.floor(metersPerSecond * 2.23694);
    }*/
    /**
     * This method converts meters per second to miles per hour.
     */
    public static int getMilesPerHour(int meters, int seconds) {
        return (int) Math.floor((meters * 2.23694) / seconds);
    }

    /**
     * This method calculates the size of the input.
     */
    public static <T> int size(Iterable<T> input) {
        if (input instanceof Collection) {
            return ((Collection) input).size();
        }
        int count = 0;
        for (Iterator<T> it = input.iterator(); it.hasNext(); it.next()) {
            count++;
        }
        return count;
    }

    /**
     * This method clears the input.
     */
    public static <T> void clear(Iterable<T> input) {
        if (input instanceof Collection) {
            ((Collection) input).clear();
        }
        for (Iterator<T> it = input.iterator(); it.hasNext(); it.next()) {
            it.remove();
        }
    }

    public final static int SEGMENT_SIZE = 5280;

    /**
     * This method calculates the segment based on the position.
     */
    public static int getSegment(int position) {
        return position / SEGMENT_SIZE;
    }

    public static <T, T2 extends DataStream<T>> DataStream<T> union(Collection<T2> items) throws Exception {

        Iterator<T2> iterator = items.iterator();
        if (!iterator.hasNext()) {
            throw new Exception("It must contains at least one item");
        }
        T2 first = iterator.next();
        List<T2> others = new ArrayList<T2>();
        while (iterator.hasNext()) {
            others.add(iterator.next());
        }
        if (others.isEmpty()) {
            return first;
        }
        return first.union(others.toArray(new DataStream[others.size()]));
    }

    public static <T, T2 extends DataStream<T>> DataStream<T> union(DataStream<T2>... items) throws Exception {
        return (DataStream<T>) union(Arrays.asList(items));
    }
}
