package master2018.flink;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.PrincipalEventBetweenSegmentsFilter;
import master2018.flink.functions.PrincipalEventTimestampExtractor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSpeedReporter {

    /**
     * Log.
     */
    private final static Logger LOG = Logger.getLogger(AverageSpeedReporter.class.getName());

    private static final List<String> PARTITION_0 = Arrays.asList("0");
    private static final List<String> PARTITION_1 = Arrays.asList("1");

    // Evaluates the speed fines.
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        SplitStream<PrincipalEvent> split = tuples
                .filter(new PrincipalEventBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .split(new OutputSelector<PrincipalEvent>() {
                    @Override
                    public Iterable<String> select(PrincipalEvent event) {
                        if (event.getDirection() == 0) {
                            return PARTITION_0;
                        } else {
                            return PARTITION_1;
                        }
                    }
                });
        split.select("0")
                .keyBy(1, 3)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .aggregate(new AggregateFunctionImpl())
                .writeAsCsv("/host/flink/ou1_0.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        split.select("1")
                .keyBy(1, 3)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .aggregate(new AggregateFunctionImpl())
                .writeAsCsv("/host/flink/ou1_1.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        return null;

        /*return tuples
                .filter(new PrincipalEventBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .keyBy(new PrincipalEventKeySelector())
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new PrincipalEventWindowFunction());*/
        // <----- PrincipalEventWindowFunction: faltan comprobaciones y utilizar AverageSpeedEvent
        //        en vez de AverageSpeedTmpEvent
        //        Tiene un rendimiento malo, pero creo que podria funcionar.
        //        Otra solucion: SlidingWindow?
        //                       reduce?
        /*.keyBy(1, 3, 5);
        .reduce(
        new ReduceFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
        @Override
        public Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value1,
        Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value2)
        throws Exception {
        return new Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
        value1.f0, value1.f1, value1.f2, value1.f3, value1.f4, value1.f5, value1.f6, value1.f7, value1.f8 + value2.f8);
        }
        });*/
    }

    private static class AggregateFunctionImpl implements AggregateFunction<PrincipalEvent, AverageSpeedTempEvent, AverageSpeedTempEvent> {

        public AggregateFunctionImpl() {
        }

        @Override
        public AverageSpeedTempEvent createAccumulator() {
            return new AverageSpeedTempEvent();
        }

        @Override
        public void add(PrincipalEvent value, AverageSpeedTempEvent accumulator) {
            if (!accumulator.isValid()) {
                accumulator.setTime1(value.getTime());
                accumulator.setTime2(value.getTime());

                accumulator.setVid(value.getVid());
                accumulator.setHighway(value.getHighway());
                accumulator.setDirection(value.getDirection());

                accumulator.setPosition1(value.getPosition());
                accumulator.setPosition2(value.getPosition());
                accumulator.setSegment1(value.getSegment());
                accumulator.setSegment2(value.getSegment());
            } else {
                //int vid = value.getVid();
                //int highway = value.getHighway();
                //int direction = value.getDirection();
                if (value.getTime() < accumulator.getTime1()) {
                    accumulator.setTime1(value.getTime());
                    accumulator.setPosition1(value.getPosition());
                    accumulator.setSegment1(value.getSegment());
                } else {
                    accumulator.setTime2(value.getTime());
                    accumulator.setPosition2(value.getPosition());
                    accumulator.setSegment2(value.getSegment());
                }
            }
        }

        @Override
        public AverageSpeedTempEvent getResult(AverageSpeedTempEvent accumulator) {
            return accumulator;
        }

        @Override
        public AverageSpeedTempEvent merge(AverageSpeedTempEvent a, AverageSpeedTempEvent b) {
            int vid = a.getVid();
            int highway = a.getHighway();
            int direction = a.getDirection();

            // Minimum
            int time1;
            int position1;
            int segment1;
            if (a.getTime1() < b.getTime1()) {
                time1 = a.getTime1();
                position1 = a.getPosition1();
                segment1 = a.getSegment1();
            } else {
                time1 = b.getTime1();
                position1 = b.getPosition1();
                segment1 = b.getSegment1();
            }

            // Maximum
            int time2;
            int position2;
            int segment2;
            if (a.getTime2() > b.getTime2()) {
                time2 = a.getTime2();
                position2 = a.getPosition2();
                segment2 = a.getSegment2();
            } else {
                time2 = b.getTime2();
                position2 = b.getPosition2();
                segment2 = b.getSegment2();
            }
            return new AverageSpeedTempEvent(time1, time2, vid, highway, direction, position1, position2, segment1, segment2);
        }
    }
}
