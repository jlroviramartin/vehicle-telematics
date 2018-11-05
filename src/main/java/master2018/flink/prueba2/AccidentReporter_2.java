package master2018.flink;

import master2018.flink.events.AccidentEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AccidentReporter_2 {

    // 39s / 38s
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .map(new MapToReducedPrincipalEventFunction())
                .filter(event -> event.getSpeed() == 0) // speed = 0
                .keyBy(ReducedPrincipalEvent.VID,
                       ReducedPrincipalEvent.HIGHWAY,
                       ReducedPrincipalEvent.DIRECTION,
                       ReducedPrincipalEvent.SEGMENT,
                       ReducedPrincipalEvent.POSITION) // key stream by vid, highway, direction, segment, position
                .countWindow(4, 1) // Slide window, start each one event
                .aggregate(new CountAccidentsAggregateFunction())
                .filter(new FilterFunction<CountableAccidentEvent>() {
                    @Override
                    public boolean filter(CountableAccidentEvent value) throws Exception {
                        return value.getCount() == 4;
                    }
                })
                .map(new MapToAccidentEventFunction());
    }

    /**
     * This tuple is a reduction of the {@code PrincipalEvent}.
     */
    private static final class ReducedPrincipalEvent
            extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

        // time - f0
        // vid - f1
        // speed - f2
        // highway - f3
        // direction - f4
        // segment - f5
        // position - f6
        public static final int VID = 1;
        public static final int HIGHWAY = 3;
        public static final int DIRECTION = 4;
        public static final int SEGMENT = 5;
        public static final int POSITION = 6;

        public ReducedPrincipalEvent() {
        }

        public ReducedPrincipalEvent(int time, int vid, int speed, int highway, int direction, int segment, int position) {
            setTime(time);
            setVid(vid);
            setSpeed(speed);
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

        public int getSpeed() {
            return f2;
        }

        public void setSpeed(int speed) {
            f2 = speed;
        }

        public int getHighway() {
            return f3;
        }

        public void setHighway(int highway) {
            f3 = highway;
        }

        public int getDirection() {
            return f4;
        }

        public void setDirection(int direction) {
            f4 = direction;
        }

        public int getSegment() {
            return f5;
        }

        public void setSegment(int segment) {
            f5 = segment;
        }

        public int getPosition() {
            return f6;
        }

        public void setPosition(int position) {
            f6 = position;
        }
    }

    /**
     * This {@code Tuple8} maintains a counter of accidents.
     */
    private static final class CountableAccidentEvent
            extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

        // time1 - f0
        // time1 - f1
        // vid - f2
        // highway - f3
        // segment - f4
        // direction - f5
        // position - f6
        // count - f7
        public CountableAccidentEvent() {
        }

        public CountableAccidentEvent(int time1, int time2, int vid, int highway, int segment, int direction, int position, int count) {
            f0 = time1;
            f1 = time2;
            f2 = vid;
            f3 = highway;
            f4 = segment;
            f5 = direction;
            f6 = position;
            f7 = count;
        }

        public int getTime1() {
            return f0;
        }

        public void setTime1(int time) {
            f0 = time;
        }

        public int getTime2() {
            return f1;
        }

        public void setTime2(int time) {
            f1 = time;
        }

        public int getVid() {
            return f2;
        }

        public void setVid(int vid) {
            f2 = vid;
        }

        public int getHighway() {
            return f3;
        }

        public void setHighway(int highway) {
            f3 = highway;
        }

        public int getSegment() {
            return f4;
        }

        public void setSegment(int segment) {
            f4 = segment;
        }

        public int getDirection() {
            return f5;
        }

        public void setDirection(int direction) {
            f5 = direction;
        }

        public int getPosition() {
            return f6;
        }

        public void setPosition(int position) {
            f6 = position;
        }

        public int getCount() {
            return f7;
        }

        public void setCount(int count) {
            f7 = count;
        }
    }

    /**
     * This {@code AggregateFunction} aggregates and counts the accidents.
     */
    private static final class CountAccidentsAggregateFunction implements AggregateFunction<ReducedPrincipalEvent, CountableAccidentEvent, CountableAccidentEvent> {

        public CountAccidentsAggregateFunction() {
        }

        @Override
        public CountableAccidentEvent createAccumulator() {
            return new CountableAccidentEvent(0, 0, 0, 0, 0, 0, 0, 0);
        }

        @Override
        public void add(ReducedPrincipalEvent value, CountableAccidentEvent accumulator) {
            if (accumulator.getCount() == 0) {
                accumulator.setCount(1);
                accumulator.setTime1(value.getTime());
                accumulator.setTime2(value.getTime());
                accumulator.setVid(value.getVid());
                accumulator.setHighway(value.getHighway());
                accumulator.setSegment(value.getSegment());
                accumulator.setDirection(value.getDirection());
                accumulator.setPosition(value.getPosition());
            } else {
                accumulator.setCount(accumulator.getCount() + 1);
                if (value.getTime() < accumulator.getTime1()) {
                    accumulator.setTime1(value.getTime());
                } else if (value.getTime() > accumulator.getTime2()) {
                    accumulator.setTime2(value.getTime());
                }
            }
        }

        @Override
        public CountableAccidentEvent getResult(CountableAccidentEvent accumulator) {
            return accumulator;
        }

        @Override
        public CountableAccidentEvent merge(CountableAccidentEvent a, CountableAccidentEvent b) {
            if (a.getCount() == 0) {
                return b;
            } else if (b.getCount() == 0) {
                return a;
            } else {
                CountableAccidentEvent accumulator = new CountableAccidentEvent();
                accumulator.setCount(a.getCount() + b.getCount());
                accumulator.setTime1(Math.min(a.getTime1(), b.getTime1()));
                accumulator.setTime1(Math.max(a.getTime2(), b.getTime2()));
                accumulator.setVid(a.getVid());
                accumulator.setHighway(a.getHighway());
                accumulator.setSegment(a.getSegment());
                accumulator.setDirection(a.getDirection());
                accumulator.setPosition(a.getPosition());
                return accumulator;
            }
        }
    }

    /**
     * This {@code MapFunction} maps from {@code PrincipalEvent} to {@code ReducedPrincipalEvent}.
     */
    @FunctionAnnotation.ForwardedFields("0;1;2;3;5->4;6->5;7->6")
    private static class MapToReducedPrincipalEventFunction implements MapFunction<PrincipalEvent, ReducedPrincipalEvent> {

        @Override
        public ReducedPrincipalEvent map(PrincipalEvent value) throws Exception {
            return new ReducedPrincipalEvent(
                    value.getTime(), // 0
                    value.getVid(), // 1
                    value.getSpeed(), // 2
                    value.getHighway(), // 3
                    value.getDirection(), // 5->4
                    value.getSegment(), // 6->5
                    value.getPosition()); // 7->6
        }
    }

    /**
     * This {@code MapFunction} maps from {@code CountableAccidentEvent} to {@code AccidentEvent}.
     */
    @FunctionAnnotation.ForwardedFields("0;1;2;3;4;5;6")
    private static class MapToAccidentEventFunction implements MapFunction<CountableAccidentEvent, AccidentEvent> {

        @Override
        public AccidentEvent map(CountableAccidentEvent value) throws Exception {
            return new AccidentEvent(
                    value.getTime1(), // 0
                    value.getTime2(), // 1
                    value.getVid(), // 2
                    value.getHighway(), // 3
                    value.getSegment(), // 4
                    value.getDirection(), // 5
                    value.getPosition()); // 6
        }
    }
}
