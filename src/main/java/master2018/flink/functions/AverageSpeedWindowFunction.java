package master2018.flink.functions;

import java.util.Iterator;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * This class evaluates the {@code PrincipalEvent}s to build {@code AverageSpeedEvent}s.
 */
public final class AverageSpeedWindowFunction
        implements WindowFunction<PrincipalEvent, AverageSpeedEvent, Tuple, TimeWindow> {

    public AverageSpeedWindowFunction() {
    }

    private static int getMilesPerHour(int metersPerSecond) {
        return (int) Math.floor(metersPerSecond * 2.23694);
    }

    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<PrincipalEvent> input,
                      Collector<AverageSpeedEvent> out) throws Exception {

        Iterator<PrincipalEvent> iterator = input.iterator();
        if (!iterator.hasNext()) {
            return;
        }
        PrincipalEvent v = iterator.next();

        int vid = v.getVid();
        int highway = v.getHighway();
        int direction = v.getDirection();

        int time1 = v.getTime(), position1 = v.getPosition(), segment1 = v.getSegment();
        int time2 = time1, position2 = position1, segment2 = segment1;

        // Early test.
        if ((direction == 0 && segment1 != 52)
            || (direction == 1 && segment1 != 56)) {
            return;
        }

        // Aggregate.
        while (iterator.hasNext()) {
            v = iterator.next();

            time2 = v.getTime();
            position2 = v.getPosition();
            segment2 = v.getSegment();

            // We can improve it!
        }

        // Last test.
        if ((direction == 0 && segment2 == 56)
            || (direction == 1 && segment2 == 52)) {

            int averageSpeed = getMilesPerHour(Math.abs(position2 - position1) / (time2 - time1));
            if (averageSpeed > 60) {

                AverageSpeedEvent result = new AverageSpeedEvent(
                        time1, time2,
                        vid, highway, direction,
                        averageSpeed);

                out.collect(result);
            }
        }
    }
}
