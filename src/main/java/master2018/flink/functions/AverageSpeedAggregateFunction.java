package master2018.flink.functions;

import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * This class aggregates {@code PrincipalEvent} events to {@code AverageSpeedTempEvent}.
 */
public final class AverageSpeedAggregateFunction implements AggregateFunction<PrincipalEvent, AverageSpeedTempEvent, AverageSpeedTempEvent> {

    public AverageSpeedAggregateFunction() {
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
            if (value.getTime() < accumulator.getTime1()) {
                accumulator.setTime1(value.getTime());
                accumulator.setPosition1(value.getPosition());
                accumulator.setSegment1(value.getSegment());
            } else if (value.getTime() > accumulator.getTime2()) {
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
        if (!a.isValid()) {
            return b;
        } else if (!b.isValid()) {
            return a;
        }

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
