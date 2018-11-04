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
     * This method convert meters per second miles per hour.
     */
    /*public static int getMilesPerHour(int metersPerSecond) {
        return (int) Math.floor(metersPerSecond * 2.23694);
    }*/

    /**
     * This method convert meters per second miles per hour.
     */
    public static int getMilesPerHour(int meters, int seconds) {
        return (int) Math.floor((meters * 2.23694) / seconds);
    }

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
}
