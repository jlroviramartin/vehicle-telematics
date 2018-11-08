package master2018.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AverageSpeedAggregateFunction;
import master2018.flink.functions.AverageSpeedBetweenSegmentsFilter;
import master2018.flink.functions.DirectionOutputSelector;
import master2018.flink.functions.PrincipalEventTimestampExtractor;
import master2018.flink.libs.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
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

    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) throws Exception {

        SplitStream<PrincipalEvent> split = tuples
                .filter(new AverageSpeedBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .split(new DirectionOutputSelector());

        List<DataStream<AverageSpeedTempEvent>> splitsByDirection = new ArrayList<DataStream<AverageSpeedTempEvent>>();
        for (String key : Arrays.asList(DirectionOutputSelector.DIRECTION_0, DirectionOutputSelector.DIRECTION_1)) {
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
    }

    /**
     * This class filter {@code AverageSpeedTempEvent} to detect fines.
     */
    private static final class AverageSpeedFinesFilterFunction implements FilterFunction<AverageSpeedTempEvent> {

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
    @FunctionAnnotation.ForwardedFields("f0; f1; f2; f3; f4")
    private static final class AverageSpeedEventMapFunction implements MapFunction<AverageSpeedTempEvent, AverageSpeedEvent> {

        public AverageSpeedEventMapFunction() {
        }

        @Override
        public AverageSpeedEvent map(AverageSpeedTempEvent value) throws Exception {
            if (value.getDirection() == 0) {
                return new AverageSpeedEvent(
                        value.getTime1(),// 0
                        value.getTime2(), // 1
                        value.getVid(), // 2
                        value.getHighway(), // 3
                        value.getDirection(), // 4
                        getMilesPerHour(value.getPosition2() - value.getPosition1(), value.getTime2() - value.getTime1())); // 5
            } else {
                return new AverageSpeedEvent(
                        value.getTime1(), // 0
                        value.getTime2(), // 1
                        value.getVid(), // 2
                        value.getHighway(), // 3
                        value.getDirection(), // 4
                        getMilesPerHour(value.getPosition1() - value.getPosition2(), value.getTime2() - value.getTime1()));
            }
        }
    }
}
