package master2018.flink.libs;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.*;

public class Utils {

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

    public static <T, T2 extends DataStream<T>> DataStream<T> union(Collection<T2> items) throws Exception {

        Iterator<T2> iterator = items.iterator();
        if (!iterator.hasNext()) {
            throw new Exception("It must contains at least one item");
        }
        T2 first = iterator.next();
        List<T2> others = new ArrayList<>();
        while (iterator.hasNext()) {
            others.add(iterator.next());
        }
        if (others.isEmpty()) {
            return first;
        }
        return first.union(others.toArray(new DataStream[others.size()]));
    }
}
