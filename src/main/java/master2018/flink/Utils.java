/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink;

import java.util.Collection;
import java.util.Iterator;

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

    /**
     * This method calculates the segment based on the position.
     */
    public static int getSegment(int position) {
        return position / 5280;
    }
}
