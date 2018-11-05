package master2018.flink;

import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static master2018.flink.Utils.getMilesPerHour;
import static master2018.flink.functions.AverageSpeedBetweenSegmentsFilter.MAX;
import static master2018.flink.functions.AverageSpeedBetweenSegmentsFilter.MIN;

// 3m 53 / 5m 14s / con paralelizacion de 10: 2m 32s
public class AverageSpeedReporter_6 {

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
        return result;
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
    private static final class AverageSpeedAggregateFunction implements AggregateFunction<ReducedPrincipalEvent, Acc, AverageSpeedEvent> {

        private static final AverageSpeedEvent EMPTY = new AverageSpeedEvent(0, 0, 0, 0, 0, 0);

        public AverageSpeedAggregateFunction() {
        }

        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        @Override
        public void add(ReducedPrincipalEvent value, Acc accumulator) {
            switch (value.getSegment()) {
                case 52: {
                    if (value.getPosition() < accumulator.position52) {
                        if (accumulator.vid == -1) {
                            accumulator.vid = value.getVid();
                            accumulator.highway = value.getHighway();
                            accumulator.direction = value.getDirection();
                        }

                        accumulator.s52 = true;
                        accumulator.time52 = value.getTime();
                        accumulator.position52 = value.getPosition();
                    }
                    break;
                }
                case 53:
                    accumulator.s53 = true;
                    break;
                case 54:
                    accumulator.s54 = true;
                    break;
                case 55:
                    accumulator.s55 = true;
                    break;
                case 56: {
                    if (value.getPosition() > accumulator.position56) {
                        accumulator.s56 = true;
                        accumulator.time56 = value.getTime();
                        accumulator.position56 = value.getPosition();
                    }
                    break;
                }
            }
        }

        @Override
        public AverageSpeedEvent getResult(Acc accumulator) {
            if (accumulator.s52 && accumulator.s53 && accumulator.s54 && accumulator.s55 && accumulator.s56) {
                // m/s -> miles/h
                if (accumulator.time52 < accumulator.time56) { // Incrementa
                    int averageSpeed = getMilesPerHour(accumulator.position56 - accumulator.position52, accumulator.time56 - accumulator.time52);
                    return new AverageSpeedEvent(accumulator.time52, accumulator.time56, accumulator.vid, accumulator.highway, 0, averageSpeed);
                } else { // Decrementa
                    int averageSpeed = getMilesPerHour(accumulator.position56 - accumulator.position52, accumulator.time52 - accumulator.time56);
                    return new AverageSpeedEvent(accumulator.time56, accumulator.time52, accumulator.vid, accumulator.highway, 1, averageSpeed);
                }
            }
            return EMPTY;
        }

        @Override
        public Acc merge(Acc a, Acc b) {
            if (a.vid == -1) {
                return b;
            } else if (b.vid == -1) {
                return a;
            }

            Acc acc = new Acc();
            acc.vid = a.vid;
            acc.highway = a.highway;
            acc.direction = a.direction;

            acc.s52 = a.s52 || b.s52;
            acc.s53 = a.s53 || b.s53;
            acc.s54 = a.s54 || b.s54;
            acc.s55 = a.s55 || b.s55;
            acc.s56 = a.s56 || b.s56;

            if (a.position52 < b.position52) {
                acc.time52 = a.time52;
                acc.position52 = a.position52;
            } else {
                acc.time52 = b.time52;
                acc.position52 = b.position52;
            }

            if (a.position56 > b.position56) {
                acc.time56 = a.time56;
                acc.position56 = a.position56;
            } else {
                acc.time56 = b.time56;
                acc.position56 = b.position56;
            }

            return acc;
        }
    }

    /**
     * This class is an accumulator.
     */
    private static final class Acc {

        public int vid = -1;
        public int highway = 0;
        public int direction = 0;

        public int time52 = 0;
        public int time56 = 0;

        public int position52 = Integer.MAX_VALUE;
        public int position56 = Integer.MIN_VALUE;

        public boolean s52 = false, s53 = false, s54 = false, s55 = false, s56 = false;
    }
}
