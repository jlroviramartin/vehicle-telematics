package master2018.flink;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AverageSpeedAggregateFunction;
import master2018.flink.functions.AverageSpeedBetweenSegmentsFilter;
import master2018.flink.functions.PrincipalEventTimestampExtractor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSpeedReporter {

    /**
     * Log.
     */
    private final static Logger LOG = Logger.getLogger(AverageSpeedReporter.class.getName());

    private static final String DIRECTION_0 = "direction0";
    private static final String DIRECTION_1 = "direction1";

    // Evaluates the speed fines.
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        // Tiempo : 7m 40s (2)
        tuples
                .filter(new AverageSpeedBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY, PrincipalEvent.DIRECTION)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .aggregate(new AverageSpeedAggregateFunction())
                .setParallelism(8)
                .filter(new AverageSpeedFinesFilterFunction())
                .map(new AverageSpeedEventMapFunction())
                .writeAsCsv("/host/flink/out.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        /* Other solutions...

        // Time : 12m 50s (3)
        tuples
                .filter(new AverageSpeedBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY, PrincipalEvent.DIRECTION)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new AverageSpeedWindowFunction())
                .setParallelism(8)
                .writeAsCsv("/host/flink/out.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // Time : Tiempo : 8m 5s (1)
        SplitStream<PrincipalEvent> split = tuples
                .filter(new AverageSpeedBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .split(new DirectionOutputSelector());

        SingleOutputStreamOperator<AverageSpeedTempEvent> split1 = split.select(DIRECTION_0)
                .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY) // We dont need direction.
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .aggregate(new AverageSpeedAggregateFunction())
                .setParallelism(4);

        SingleOutputStreamOperator<AverageSpeedTempEvent> split2 = split.select(DIRECTION_1)
                .keyBy(PrincipalEvent.VID, PrincipalEvent.HIGHWAY) // We dont need direction.
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .aggregate(new AverageSpeedAggregateFunction())
                .setParallelism(4);

        split1.union(split2)
                .filter(new AverageSpeedFinesFilterFunction())
                .map(new AverageSpeedEventMapFunction())
                .writeAsCsv("/host/flink/out.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
         */
        return null;
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
     * This method convert meters per second miles per hour.
     */
    public static int getMilesPerHour(int metersPerSecond) {
        return (int) Math.floor(metersPerSecond * 2.23694);
    }

    /**
     * This class filter AverageSpeedTempEvent to detect fines.
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
                int averageSpeed = getMilesPerHour(value.getPosition2() - value.getPosition1()) / (value.getTime2() - value.getTime1());
                return (averageSpeed > 60);
            } else {
                if (value.getSegment1() != 56 || value.getSegment2() != 52) {
                    return false;
                }
                // m/s -> miles/h
                int averageSpeed = getMilesPerHour(value.getPosition1() - value.getPosition2()) / (value.getTime2() - value.getTime1());
                return (averageSpeed > 60);
            }
        }
    }

    /**
     * This class maps from AverageSpeedTempEvent to AverageSpeedEvent;
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
                        getMilesPerHour(value.getPosition2() - value.getPosition1()) / (value.getTime2() - value.getTime1()));
            } else {
                return new AverageSpeedEvent(
                        value.getTime1(),
                        value.getTime2(),
                        value.getVid(),
                        value.getHighway(),
                        value.getDirection(),
                        getMilesPerHour(value.getPosition1() - value.getPosition2()) / (value.getTime2() - value.getTime1()));
            }
        }
    }
}
