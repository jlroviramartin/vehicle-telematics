package master2018.flink;

import java.nio.file.Paths;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static master2018.flink.Utils.getMilesPerHour;
import static master2018.flink.functions.AverageSpeedBetweenSegmentsFilter.MAX;
import static master2018.flink.functions.AverageSpeedBetweenSegmentsFilter.MIN;

// 4m 23s / 3m 52s / 3m 38s / 3m 38s / 4m 57s / 4m 42s / 4m 58s
public class AverageSpeedReporter_3 {

    // Evaluates the speed fines.
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        SingleOutputStreamOperator<AverageSpeedEvent> result = tuples
                .map(new MapToReducedPrincipalEventFunction())
                .filter(new FilterFunction<ReducedPrincipalEvent>() {
                    @Override
                    public boolean filter(ReducedPrincipalEvent value) throws Exception {
                        int segment = value.getSegment();
                        return segment >= MIN && segment <= MAX;
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReducedPrincipalEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReducedPrincipalEvent element) {
                        return element.getTime() * 1000;
                    }
                })
                .keyBy(ReducedPrincipalEvent.VID, ReducedPrincipalEvent.HIGHWAY, ReducedPrincipalEvent.DIRECTION)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .aggregate(new AverageSpeedAggregateFunction())
                .setParallelism(10)
                .filter(new AverageSpeedFinesFilterFunction());

        String outputPath = Paths.get(VehicleTelematics.DEBUG_OUTPUTPATH).toString();

        result.writeAsCsv(Paths.get(outputPath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        return null;
    }

    /**
     * This {@code Tuple6} is a reduction of the {@code PrincipalEvent}.
     */
    public static final class ReducedPrincipalEvent
            extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {

        // time - f0
        // vid - f1
        // highway - f2
        // direction - f3
        // segment - f4
        // position - f5
        public static final int VID = 1;
        public static final int HIGHWAY = 2;
        public static final int DIRECTION = 3;
        public static final int SEGMENT = 4;

        public ReducedPrincipalEvent() {
        }

        public ReducedPrincipalEvent(int time, int vid, int highway, int direction, int segment, int position) {
            setTime(time);
            setVid(vid);
            setHighway(highway);
            setDirection(direction);
            setSegment(segment);
            setPosition(position);
        }

        public int getTime() {
            return f0;
        }

        public void setTime(int time) {
            f0 = time;
        }

        public int getVid() {
            return f1;
        }

        public void setVid(int vid) {
            f1 = vid;
        }

        public int getHighway() {
            return f2;
        }

        public void setHighway(int highway) {
            f2 = highway;
        }

        public int getDirection() {
            return f3;
        }

        public void setDirection(int direction) {
            f3 = direction;
        }

        public int getSegment() {
            return f4;
        }

        public void setSegment(int segment) {
            f4 = segment;
        }

        public int getPosition() {
            return f5;
        }

        public void setPosition(int position) {
            f5 = position;
        }

        public void set(ReducedPrincipalEvent ev) {
            setTime(ev.getTime());
            setVid(ev.getVid());
            setHighway(ev.getHighway());
            setDirection(ev.getDirection());
            setSegment(ev.getSegment());
            setPosition(ev.getPosition());
        }

        public boolean isValid() {
            return this.getTime() >= 0;
        }

        public static final ReducedPrincipalEvent EMPTY = new ReducedPrincipalEvent(-1, 0, 0, 0, 0, 0);
    }

    /**
     * This {@code MapFunction} maps from {@code PrincipalEvent} to {@code ReducedPrincipalEvent}.
     */
    @FunctionAnnotation.ForwardedFields("0;1;3->2;5->3;6->4;7->5")
    private static final class MapToReducedPrincipalEventFunction
            implements MapFunction<PrincipalEvent, ReducedPrincipalEvent> {

        public MapToReducedPrincipalEventFunction() {
        }

        @Override
        public ReducedPrincipalEvent map(PrincipalEvent value) throws Exception {
            return new ReducedPrincipalEvent(value.getTime(), // 0->0
                                             value.getVid(), // 1->1
                                             value.getHighway(), // 3->2
                                             value.getDirection(), // 5->3
                                             value.getSegment(), // 6->4
                                             value.getPosition()); // 7->5
        }
    }

    /**
     * This class filter AverageSpeedTempEvent to detect fines.
     */
    private static final class AverageSpeedFinesFilterFunction
            implements FilterFunction<AverageSpeedEvent> {

        public AverageSpeedFinesFilterFunction() {
        }

        @Override
        public boolean filter(AverageSpeedEvent value) throws Exception {
            return value.getAverageSpeed() > 60;
        }
    }

    /**
     * This {@code AggregateFunction} aggregates the {@code ReducedPrincipalEvent} events to build
     * {@code AverageSpeedEvent}s.
     */
    private static final class AverageSpeedAggregateFunction implements AggregateFunction<ReducedPrincipalEvent, ReducedPrincipalEvent[], AverageSpeedEvent> {

        private static final AverageSpeedEvent EMPTY = new AverageSpeedEvent(0, 0, 0, 0, 0, 0);

        public AverageSpeedAggregateFunction() {
        }

        @Override
        public ReducedPrincipalEvent[] createAccumulator() {
            return new ReducedPrincipalEvent[2];
        }

        @Override
        public void add(ReducedPrincipalEvent value, ReducedPrincipalEvent[] accumulator) {
            if (accumulator[0] == null) {
                accumulator[0] = value;
                accumulator[1] = value;
            } else {
                if (value.getTime() < accumulator[0].getTime()) {
                    accumulator[0] = value;
                } else if (value.getTime() > accumulator[1].getTime()) {
                    accumulator[1] = value;
                }
            }
        }

        @Override
        public AverageSpeedEvent getResult(ReducedPrincipalEvent[] accumulator) {
            if (accumulator[0] != null) {
                if (accumulator[0].getPosition() < accumulator[1].getPosition()) { // Creciente

                    if (accumulator[0].getSegment() == 52 && accumulator[1].getSegment() == 56) {
                        int averageSpeed = getMilesPerHour(accumulator[1].getPosition() - accumulator[0].getPosition(),
                                                           accumulator[1].getTime() - accumulator[0].getTime());

                        return new AverageSpeedEvent(accumulator[0].getTime(), accumulator[1].getTime(),
                                                     accumulator[0].getVid(), accumulator[0].getHighway(), 0,
                                                     averageSpeed);
                    }
                } else { // Decreciente

                    if (accumulator[0].getSegment() == 56 && accumulator[1].getSegment() == 52) {
                        int averageSpeed = getMilesPerHour(accumulator[0].getPosition() - accumulator[1].getPosition(),
                                                           accumulator[1].getTime() - accumulator[0].getTime());

                        return new AverageSpeedEvent(accumulator[0].getTime(), accumulator[1].getTime(),
                                                     accumulator[0].getVid(), accumulator[0].getHighway(), 1,
                                                     averageSpeed);
                    }
                }
            }
            return EMPTY;
        }

        @Override
        public ReducedPrincipalEvent[] merge(ReducedPrincipalEvent[] a, ReducedPrincipalEvent[] b) {
            throw new Error("No implementado");
        }
    }
}
