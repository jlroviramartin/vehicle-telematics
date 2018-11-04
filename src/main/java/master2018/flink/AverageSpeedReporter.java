package master2018.flink;

import java.nio.file.Paths;
import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.AverageSpeedTempEvent;
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

// 4m 17s / 3m 56s / 4m 6s / 3m 53s / 3m 54s / 3m 43s <--------------------------------- GANADOR
public class AverageSpeedReporter {

    // Evaluates the speed fines.
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        SingleOutputStreamOperator<AverageSpeedEvent> result = tuples
                .map(new MapFunctionImpl())
                .filter(new FilterFunction<MyEvent>() {
                    @Override
                    public boolean filter(MyEvent value) throws Exception {
                        int segment = value.getSegment();
                        return segment >= MIN && segment <= MAX;
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MyEvent>() {
                    @Override
                    public long extractAscendingTimestamp(MyEvent element) {
                        return element.getTime() * 1000;
                    }
                })
                .keyBy(MyEvent.VID, MyEvent.HIGHWAY, MyEvent.DIRECTION)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .aggregate(new MyAggregateFunction())
                .setParallelism(8)
                .filter(new AverageSpeedFinesFilterFunction());

        String outputPath = Paths.get(VehicleTelematics.DEBUG_OUTPUTPATH).toString();

        result.writeAsCsv(Paths.get(outputPath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        return null;
    }

    public static final class MyEvent
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

        public MyEvent() {
        }

        public MyEvent(int time, int vid, int highway, int direction, int segment, int position) {
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

        public void set(MyEvent ev) {
            this.f0 = ev.f0;
            this.f1 = ev.f1;
            this.f2 = ev.f2;
            this.f3 = ev.f3;
            this.f4 = ev.f4;
            this.f5 = ev.f5;
        }

        public boolean isValid() {
            return this.getTime() >= 0;
        }

        public static final MyEvent EMPTY = new MyEvent(-1, 0, 0, 0, 0, 0);
    }

    @FunctionAnnotation.ForwardedFields("0;1;3->2;5->3;6->4;7->5")
    private static final class MapFunctionImpl implements MapFunction<PrincipalEvent, MyEvent> {

        public MapFunctionImpl() {
        }

        @Override
        public MyEvent map(PrincipalEvent value) throws Exception {
            return new MyEvent(value.getTime(), // 0->0
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
    private static final class AverageSpeedFinesFilterFunction implements FilterFunction<AverageSpeedEvent> {

        public AverageSpeedFinesFilterFunction() {
        }

        @Override
        public boolean filter(AverageSpeedEvent value) throws Exception {
            return value.getAverageSpeed() > 60;
        }
    }

    // 5m 4s / 4m 43s
    public static final class MyAggregateFunction implements AggregateFunction<MyEvent, AverageSpeedTempEvent, AverageSpeedEvent> {

        private static final AverageSpeedEvent EMPTY = new AverageSpeedEvent(0, 0, 0, 0, 0, 0);

        public MyAggregateFunction() {
        }

        @Override
        public AverageSpeedTempEvent createAccumulator() {
            return new AverageSpeedTempEvent();
        }

        @Override
        public void add(MyEvent value, AverageSpeedTempEvent accumulator) {
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
        public AverageSpeedEvent getResult(AverageSpeedTempEvent accumulator) {
            if (accumulator.getDirection() == 0) {
                if (accumulator.getSegment1() != 52 || accumulator.getSegment2() != 56) {
                    return EMPTY;
                }
                // m/s -> miles/h
                int averageSpeed = getMilesPerHour(accumulator.getPosition2() - accumulator.getPosition1(), accumulator.getTime2() - accumulator.getTime1());
                if (averageSpeed <= 60) {
                    return EMPTY;
                }
                return new AverageSpeedEvent(
                        accumulator.getTime1(),
                        accumulator.getTime2(),
                        accumulator.getVid(),
                        accumulator.getHighway(),
                        accumulator.getDirection(),
                        averageSpeed);
            } else {
                if (accumulator.getSegment1() != 56 || accumulator.getSegment2() != 52) {
                    return EMPTY;
                }
                // m/s -> miles/h
                int averageSpeed = getMilesPerHour(accumulator.getPosition1() - accumulator.getPosition2(), accumulator.getTime2() - accumulator.getTime1());
                if (averageSpeed <= 60) {
                    return EMPTY;
                }
                return new AverageSpeedEvent(
                        accumulator.getTime1(),
                        accumulator.getTime2(),
                        accumulator.getVid(),
                        accumulator.getHighway(),
                        accumulator.getDirection(),
                        averageSpeed);
            }
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

    // 3m 53
    /*private static final class MyAggregateFunction implements AggregateFunction<MyEvent, Acc, AverageSpeedEvent> {

        private static final AverageSpeedEvent EMPTY = new AverageSpeedEvent(0, 0, 0, 0, 0, 0);

        public MyAggregateFunction() {
        }

        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        @Override
        public void add(MyEvent value, Acc accumulator) {
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
            throw new Error("No implementado");
        }
    }

    private static final class Acc {

        public int vid = -1;
        public int highway = 0;
        public int direction = 0;

        public int time52 = 0;
        public int time56 = 0;

        public int position52 = Integer.MAX_VALUE;
        public int position56 = Integer.MIN_VALUE;

        public boolean s52 = false, s53 = false, s54 = false, s55 = false, s56 = false;
    }*/
    // 4m 23s / 3m 52s / 3m 38s / 3m 38s / 4m 57s / 4m 42s / 4m 58s
    /*private static final class MyAggregateFunction implements AggregateFunction<MyEvent, MyEvent[], AverageSpeedEvent> {

        private static final AverageSpeedEvent EMPTY = new AverageSpeedEvent(0, 0, 0, 0, 0, 0);

        public MyAggregateFunction() {
        }

        @Override
        public MyEvent[] createAccumulator() {
            return new MyEvent[2];
        }

        @Override
        public void add(MyEvent value, MyEvent[] accumulator) {
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
        public AverageSpeedEvent getResult(MyEvent[] accumulator) {
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
        public MyEvent[] merge(MyEvent[] a, MyEvent[] b) {
            throw new Error("No implementado");
        }
    }*/
}
