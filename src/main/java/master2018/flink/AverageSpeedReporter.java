package master2018.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AverageSpeedAggregateFunction;
import master2018.flink.functions.AverageSpeedBetweenSegmentsFilter;
import master2018.flink.functions.PrincipalEventTimestampExtractor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static master2018.flink.Utils.getMilesPerHour;

// 1m 14s
public class AverageSpeedReporter {

    private static final String DIRECTION_0 = "direction0";
    private static final String DIRECTION_1 = "direction1";

    // Evaluates the speed fines.
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) throws Exception {

        // Time : Tiempo : 8m 5s (1)
        SplitStream<PrincipalEvent> split = tuples
                .filter(new AverageSpeedBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .split(new DirectionOutputSelector());

        List<DataStream<AverageSpeedTempEvent>> splitsByDirection = new ArrayList<DataStream<AverageSpeedTempEvent>>();
        for (String key : Arrays.asList(DIRECTION_0, DIRECTION_1)) {
            SingleOutputStreamOperator<AverageSpeedTempEvent> splitByDirection = split.select(key)
                    .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY) // We dont need direction.
                    .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                    .aggregate(new AverageSpeedAggregateFunction())
                    .setParallelism(10);
            splitsByDirection.add(splitByDirection);
        }
        return Utils.union(splitsByDirection)
                .filter(new AverageSpeedFinesFilterFunction())
                .map(new AverageSpeedEventMapFunction());

        /*SingleOutputStreamOperator<AverageSpeedTempEvent> split1 = split.select(DIRECTION_0)
                .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY) // We dont need direction.
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .aggregate(new AverageSpeedAggregateFunction())
                .setParallelism(10);

        SingleOutputStreamOperator<AverageSpeedTempEvent> split2 = split.select(DIRECTION_1)
                .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY) // We dont need direction.
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .aggregate(new AverageSpeedAggregateFunction())
                .setParallelism(10);

        return split1.union(split2)
                .filter(new AverageSpeedFinesFilterFunction())
                .map(new AverageSpeedEventMapFunction());*/
    }

    /**
     * This class selects {@code PrincipalEvent}s based on the direction. It is only used in case of splitting the
     * original data ({@code tuples} or list of {@code PrincipalEvent})
     */
    private static class DirectionOutputSelector implements OutputSelector<PrincipalEvent> {

        public DirectionOutputSelector() {
        }

        private static final List<String> PARTITION_DIR_0 = Arrays.asList(DIRECTION_0);
        private static final List<String> PARTITION_DIR_1 = Arrays.asList(DIRECTION_1);

        @Override
        public Iterable<String> select(PrincipalEvent event) {
            if (event.getDirection() == 0) {
                return PARTITION_DIR_0;
            } else {
                return PARTITION_DIR_1;
            }
        }
    }

    /**
     * This class filter {@code AverageSpeedTempEvent} to detect fines.
     */
    private static class AverageSpeedFinesFilterFunction implements FilterFunction<AverageSpeedTempEvent> {

        public AverageSpeedFinesFilterFunction() {
        }

        @Override
        public boolean filter(AverageSpeedTempEvent value) throws Exception {
            if (value.getDirection() == 0) {
                if (value.getSegment1() != 52 || value.getSegment2() != 56) {
                    return false;
                }
                // m/s -> miles/h
                int averageSpeed = getMilesPerHour(value.getPosition2() - value.getPosition1(), value.getTime2() - value.getTime1());
                return (averageSpeed > 60);
            } else {
                if (value.getSegment1() != 56 || value.getSegment2() != 52) {
                    return false;
                }
                // m/s -> miles/h
                int averageSpeed = getMilesPerHour(value.getPosition1() - value.getPosition2(), value.getTime2() - value.getTime1());
                return (averageSpeed > 60);
            }
        }
    }

    /**
     * This class maps from {@code AverageSpeedTempEvent} to {@code AverageSpeedEvent};
     */
    private static class AverageSpeedEventMapFunction implements MapFunction<AverageSpeedTempEvent, AverageSpeedEvent> {

        public AverageSpeedEventMapFunction() {
        }

        @Override
        public AverageSpeedEvent map(AverageSpeedTempEvent value) throws Exception {
            if (value.getDirection() == 0) {
                return new AverageSpeedEvent(
                        value.getTime1(),
                        value.getTime2(),
                        value.getVid(),
                        value.getHighway(),
                        value.getDirection(),
                        getMilesPerHour(value.getPosition2() - value.getPosition1(), value.getTime2() - value.getTime1()));
            } else {
                return new AverageSpeedEvent(
                        value.getTime1(),
                        value.getTime2(),
                        value.getVid(),
                        value.getHighway(),
                        value.getDirection(),
                        getMilesPerHour(value.getPosition1() - value.getPosition2(), value.getTime2() - value.getTime1()));
            }
        }
    }
}
