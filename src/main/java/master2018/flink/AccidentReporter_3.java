package master2018.flink;

import java.util.Iterator;
import master2018.flink.events.AccidentEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class AccidentReporter_3 {

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
                .apply(new CountAccidentsWindowFunction());
    }

    /**
     * This {@code Tuple7} is a reduction of the {@code PrincipalEvent}.
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
     * This {@code WindowFunction} aggregates and counts the accidents.
     */
    private static final class CountAccidentsWindowFunction
            implements WindowFunction<ReducedPrincipalEvent, AccidentEvent, Tuple, GlobalWindow> {

        @Override
        public void apply(Tuple key, GlobalWindow globalWindow, Iterable<ReducedPrincipalEvent> iterable, Collector<AccidentEvent> accidents) {

            if (Utils.size(iterable) == 4) {

                Iterator<ReducedPrincipalEvent> events = iterable.iterator();

                ReducedPrincipalEvent firstEvent = events.next();
                int time1 = firstEvent.getTime();

                ReducedPrincipalEvent lastEvent = firstEvent; // Avoid "can be null"
                while (events.hasNext()) {
                    lastEvent = events.next();
                }

                AccidentEvent accidentEvent = new AccidentEvent();
                accidentEvent.setTime1(time1);
                accidentEvent.setTime2(lastEvent.getTime());
                accidentEvent.setVid(firstEvent.getVid());
                accidentEvent.setHighway(firstEvent.getHighway());
                accidentEvent.setSegment(firstEvent.getSegment());
                accidentEvent.setDirection(firstEvent.getDirection());
                accidentEvent.setPosition(firstEvent.getPosition());

                accidents.collect(accidentEvent);
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
}
