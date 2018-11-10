package master2018.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AverageSpeedBetweenSegmentsFilter;
import master2018.flink.functions.DirectionOutputSelector;
import master2018.flink.functions.PrincipalEventTimestampExtractor;
import master2018.flink.libs.Utils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static master2018.flink.libs.Utils.getMilesPerHour;

/**
 * This class evaluates the speed fines between the segments 52 an 56.
 */
public final class AverageSpeedReporter {

    public static DataStream analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) throws Exception {

        SplitStream<PrincipalEvent> split = tuples
                .filter(new AverageSpeedBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .split(new DirectionOutputSelector());

        List<DataStream<AverageSpeedEvent>> splitsByDirection = new ArrayList<DataStream<AverageSpeedEvent>>();
        for (String key : Arrays.asList(DirectionOutputSelector.DIRECTION_0, DirectionOutputSelector.DIRECTION_1)) {
            SingleOutputStreamOperator<AverageSpeedEvent> splitByDirection = split
                    .select(key)
                    .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY) // We dont need direction.
                    .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                    .aggregate(
                            key.equals(DirectionOutputSelector.DIRECTION_0)
                            ? new EastAverageSpeedAggregateFunction()
                            : new WestAverageSpeedAggregateFunction())
                    .setParallelism(10)
                    .filter(new AverageSpeedFilterFunction())
                    .setParallelism(10);
            splitsByDirection.add(splitByDirection);
        }

        return Utils.union(splitsByDirection);
    }

    /**
     * This class filters vehicles that drive faster then 60mph.
     */
    private static final class AverageSpeedFilterFunction implements FilterFunction<AverageSpeedEvent> {

        private final static byte SPEED = 60;

        public AverageSpeedFilterFunction() {
        }

        @Override
        public boolean filter(AverageSpeedEvent value) throws Exception {
            return value.getAverageSpeed() > SPEED;
        }
    }

    /**
     * This class aggregates the data for vehicles thar drive to the East.
     */
    public static final class EastAverageSpeedAggregateFunction implements AggregateFunction<PrincipalEvent, AverageSpeedTempEvent, AverageSpeedEvent> {

        private static final int EMPTY = -1;
        private static final int CANCEL = -2;

        private final static byte MIN = 52;
        private final static byte MAX = 56;
        private final static byte SPEED = 60;

        private static final AverageSpeedEvent ERROR = new AverageSpeedEvent(0, 0, 0, 0, (byte) 0, (byte) 0);

        public EastAverageSpeedAggregateFunction() {
        }

        @Override
        public AverageSpeedTempEvent createAccumulator() {
            AverageSpeedTempEvent event = new AverageSpeedTempEvent();
            event.setTime1(EMPTY);
            return event;
        }

        @Override
        public void add(PrincipalEvent value, AverageSpeedTempEvent accumulator) {
            int time1 = accumulator.getTime1();

            switch (time1) {
                case CANCEL:
                    return;

                case EMPTY:

                    // Starts at MIN when driving to East or MAX when driving to West.
                    if (value.getSegment() != MIN) {
                        accumulator.setTime1(CANCEL);
                        return;
                    }

                    accumulator.setInitial(
                            value.getTime(),
                            value.getPosition(),
                            value.getSegment(),
                            value.getVid(),
                            value.getHighway(),
                            value.getDirection());
                    break;

                default:
                    accumulator.update(
                            value.getTime(),
                            value.getPosition(),
                            value.getSegment());
                    break;
            }
        }

        @Override
        public AverageSpeedEvent getResult(AverageSpeedTempEvent accumulator) {
            int time1 = accumulator.getTime1();
            if (time1 >= 0) {
                // Ends at MAX when driving to East or MIN when driving to West.
                // Evaluates the average speed: m/s -> miles/h
                if (accumulator.getSegment2() != MAX) {
                    return ERROR;
                }
                byte averageSpeed = getMilesPerHour(
                        accumulator.getPosition2() - accumulator.getPosition1(),
                        accumulator.getTime2() - accumulator.getTime1());

                if (averageSpeed > SPEED) {
                    return new AverageSpeedEvent(
                            accumulator.getTime1(),
                            accumulator.getTime2(),
                            accumulator.getVid(),
                            accumulator.getHighway(),
                            accumulator.getDirection(),
                            averageSpeed);
                }
            }
            return ERROR;
        }

        @Override
        public AverageSpeedTempEvent merge(AverageSpeedTempEvent a, AverageSpeedTempEvent b) {
            throw new Error();

        }
    }

    /**
     * This class aggregates the data for vehicles thar drive to the Weast.
     */
    public static final class WestAverageSpeedAggregateFunction implements AggregateFunction<PrincipalEvent, AverageSpeedTempEvent, AverageSpeedEvent> {

        private static final int EMPTY = -1;
        private static final int CANCEL = -2;

        private final static byte MIN = 52;
        private final static byte MAX = 56;
        private final static byte SPEED = 60;

        private static final AverageSpeedEvent ERROR = new AverageSpeedEvent(0, 0, 0, 0, (byte) 0, (byte) 0);

        public WestAverageSpeedAggregateFunction() {
        }

        @Override
        public AverageSpeedTempEvent createAccumulator() {
            AverageSpeedTempEvent event = new AverageSpeedTempEvent();
            event.setTime1(EMPTY);
            return event;
        }

        @Override
        public void add(PrincipalEvent value, AverageSpeedTempEvent accumulator) {
            int time1 = accumulator.getTime1();

            switch (time1) {
                case CANCEL:
                    return;

                case EMPTY:

                    // Starts at MIN when driving to East or MAX when driving to West.
                    if (value.getSegment() != MAX) {
                        accumulator.setTime1(CANCEL);
                        return;
                    }

                    accumulator.setInitial(
                            value.getTime(),
                            value.getPosition(),
                            value.getSegment(),
                            value.getVid(),
                            value.getHighway(),
                            value.getDirection());
                    break;

                default:
                    accumulator.update(
                            value.getTime(),
                            value.getPosition(),
                            value.getSegment());
                    break;
            }
        }

        @Override
        public AverageSpeedEvent getResult(AverageSpeedTempEvent accumulator) {
            int time1 = accumulator.getTime1();
            if (time1 >= 0) {
                // Ends at MAX when driving to East or MIN when driving to West.
                // Evaluates the average speed: m/s -> miles/h
                if (accumulator.getSegment2() != MIN) {
                    return ERROR;
                }
                byte averageSpeed = getMilesPerHour(
                        accumulator.getPosition1() - accumulator.getPosition2(),
                        accumulator.getTime2() - accumulator.getTime1());

                if (averageSpeed > SPEED) {
                    return new AverageSpeedEvent(
                            accumulator.getTime1(),
                            accumulator.getTime2(),
                            accumulator.getVid(),
                            accumulator.getHighway(),
                            accumulator.getDirection(),
                            averageSpeed);
                }
            }
            return ERROR;
        }

        @Override
        public AverageSpeedTempEvent merge(AverageSpeedTempEvent a, AverageSpeedTempEvent b) {
            throw new Error();
        }
    }
}
