package master2018.flink;

import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static master2018.flink.Utils.getMilesPerHour;
import static master2018.flink.functions.AverageSpeedBetweenSegmentsFilter.MAX;
import static master2018.flink.functions.AverageSpeedBetweenSegmentsFilter.MIN;

// con paralelizacion de 10: 2m 43s, 2m 53s, 2m 41s
public class AverageSpeedReporter_5 {

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
        //public static final int SEGMENT = 4;

        public ReducedPrincipalEvent() {
        }

        public ReducedPrincipalEvent(int time, int vid, int highway, int direction, int position) {
            setTime(time);
            setVid(vid);
            setHighway(highway);
            setDirection(direction);
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
            return Utils.getSegment(getPosition());
        }

        public int getPosition() {
            return f4;
        }

        public void setPosition(int position) {
            f4 = position;
        }

        public void set(ReducedPrincipalEvent ev) {
            setTime(ev.getTime());
            setVid(ev.getVid());
            setHighway(ev.getHighway());
            setDirection(ev.getDirection());
            setPosition(ev.getPosition());
        }

        public boolean isValid() {
            return this.getTime() >= 0;
        }

        public static final ReducedPrincipalEvent EMPTY = new ReducedPrincipalEvent(-1, 0, 0, 0, 0);
    }

    /**
     * This {@code MapFunction} maps from {@code PrincipalEvent} to {@code ReducedPrincipalEvent}.
     */
    @FunctionAnnotation.ForwardedFields("0;1;3->2;5->3;7->4")
    private static final class MapToReducedPrincipalEventFunction
            implements MapFunction<PrincipalEvent, ReducedPrincipalEvent> {

        @Override
        public ReducedPrincipalEvent map(PrincipalEvent value) throws Exception {
            return new ReducedPrincipalEvent(value.getTime(), // 0->0
                                             value.getVid(), // 1->1
                                             value.getHighway(), // 3->2
                                             value.getDirection(), // 5->3
                                             value.getPosition()); // 7->4
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
    private static final class AverageSpeedAggregateFunction
            implements AggregateFunction<ReducedPrincipalEvent, Acc, AverageSpeedEvent> {

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
                    if (value.getPosition() < accumulator.getPosition52()) {
                        if (accumulator.getVid() == -1) {
                            accumulator.setVid(value.getVid());
                            accumulator.setHighway(value.getHighway());
                            accumulator.setDirection(value.getDirection());
                        }

                        accumulator.setSegment52(true);
                        accumulator.setTime52(value.getTime());
                        accumulator.setPosition52(value.getPosition());
                    }
                    break;
                }
                case 53:
                    accumulator.setSegment53(true);
                    break;
                case 54:
                    accumulator.setSegment54(true);
                    break;
                case 55:
                    accumulator.setSegment55(true);
                    break;
                case 56: {
                    if (value.getPosition() > accumulator.getPosition56()) {
                        accumulator.setSegment56(true);
                        accumulator.setTime56(value.getTime());
                        accumulator.setPosition56(value.getPosition());
                    }
                    break;
                }
            }
        }

        @Override
        public AverageSpeedEvent getResult(Acc accumulator) {
            if (accumulator.containsAllSegments()) {
                // m/s -> miles/h
                if (accumulator.getTime52() < accumulator.getTime56()) { // Direction East
                    int averageSpeed = getMilesPerHour(accumulator.getPosition56() - accumulator.getPosition52(),
                                                       accumulator.getTime56() - accumulator.getTime52());

                    return new AverageSpeedEvent(accumulator.getTime52(),
                                                 accumulator.getTime56(),
                                                 accumulator.getVid(),
                                                 accumulator.getHighway(),
                                                 0,
                                                 averageSpeed);
                } else { // Direction West
                    int averageSpeed = getMilesPerHour(accumulator.getPosition56() - accumulator.getPosition52(),
                                                       accumulator.getTime52() - accumulator.getTime56());

                    return new AverageSpeedEvent(accumulator.getTime56(),
                                                 accumulator.getTime52(),
                                                 accumulator.getVid(),
                                                 accumulator.getHighway(),
                                                 1,
                                                 averageSpeed);
                }
            }
            return EMPTY;
        }

        @Override
        public Acc merge(Acc a, Acc b) {
            if (a.getVid() == -1) {
                return b;
            } else if (b.getVid() == -1) {
                return a;
            }

            Acc acc = new Acc();
            acc.setVid(a.getVid());
            acc.setHighway(a.getHighway());
            acc.setDirection(a.getDirection());

            acc.setSegmentMask(a.getSegmentMask() | b.getSegmentMask());

            // Segment 52
            if (a.getPosition52() < b.getPosition52()) {
                acc.setTime52(a.getTime52());
                acc.setPosition52(a.getPosition52());
            } else {
                acc.setTime52(b.getTime52());
                acc.setPosition52(b.getPosition52());
            }

            // Segment 56
            if (a.getPosition56() > b.getPosition56()) {
                acc.setTime56(a.getTime56());
                acc.setPosition56(a.getPosition56());
            } else {
                acc.setTime56(b.getTime56());
                acc.setPosition56(b.getPosition56());
            }

            return acc;
        }
    }

    /**
     * This class is an accumulator for {@code AverageSpeedAggregateFunction}.
     */
    public static final class Acc
            extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

        public Acc() {
            super(-1, 0, 0, 0, 0, Integer.MAX_VALUE, Integer.MIN_VALUE, 0);
        }

        private static final int MASK_52 = 1;
        private static final int MASK_53 = 2;
        private static final int MASK_54 = 4;
        private static final int MASK_55 = 8;
        private static final int MASK_56 = 16;
        private static final int ALL_MASKS = MASK_52 | MASK_53 | MASK_54 | MASK_55 | MASK_56;

        private boolean getMask(int mask) {
            return ((f7 & mask) != mask);
        }

        private void setMask(int mask, boolean value) {
            if (value) {
                f7 = (f7 | mask);
            } else {
                f7 = (f7 & ~mask);
            }
        }

        public int getVid() {
            return f0;
        }

        public void setVid(int vid) {
            f0 = vid;
        }

        public int getHighway() {
            return f1;
        }

        public void setHighway(int highway) {
            f1 = highway;
        }

        public int getDirection() {
            return f2;
        }

        public void setDirection(int direction) {
            f2 = direction;
        }

        public int getTime52() {
            return f3;
        }

        public void setTime52(int time52) {
            f3 = time52;
        }

        public int getTime56() {
            return f4;
        }

        public void setTime56(int time56) {
            f4 = time56;
        }

        public int getPosition52() {
            return f5;
        }

        public void setPosition52(int position52) {
            f5 = position52;
        }

        public int getPosition56() {
            return f6;
        }

        public void setPosition56(int position56) {
            f6 = position56;
        }

        public boolean containsAllSegments() {
            return ((f7 & ALL_MASKS) == ALL_MASKS);
        }

        public int getSegmentMask() {
            return f7;
        }

        public void setSegmentMask(int mask) {
            f7 = mask;
        }

        public boolean getSegment52() {
            return getMask(MASK_52);
        }

        public void setSegment52(boolean s52) {
            setMask(MASK_52, s52);
        }

        public boolean getSegment53() {
            return getMask(MASK_53);
        }

        public void setSegment53(boolean s53) {
            setMask(MASK_53, s53);
        }

        public boolean getSegment54() {
            return getMask(MASK_54);
        }

        public void setSegment54(boolean s54) {
            setMask(MASK_54, s54);
        }

        public boolean getSegment55() {
            return getMask(MASK_55);
        }

        public void setSegment55(boolean s55) {
            setMask(MASK_55, s55);
        }

        public boolean getSegment56() {
            return getMask(MASK_56);
        }

        public void setSegment56(boolean s56) {
            setMask(MASK_56, s56);
        }
    }
}
