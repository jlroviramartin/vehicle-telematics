package master2018.flink.functions;

import master2018.flink.events.AverageSpeedEvent;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import static master2018.flink.libs.Utils.getMilesPerHour;

/**
 * This class aggregates {@code PrincipalEvent} events to {@code AverageSpeedTempEvent}.
 */
public final class AverageSpeedAggregateFunction implements AggregateFunction<PrincipalEvent, AverageSpeedTempEvent, AverageSpeedEvent> {

    private static final int EMPTY = -1;
    private static final int CANCEL = -2;

    private final static byte MIN = 52;
    private final static byte MAX = 56;
    private final static byte SPEED = 60;

    private static final AverageSpeedEvent ERROR = new AverageSpeedEvent(0, 0, 0, 0, (byte) 0, (byte) 0);

    public AverageSpeedAggregateFunction() {
    }

    @Override
    public AverageSpeedTempEvent createAccumulator() {
        AverageSpeedTempEvent event = new AverageSpeedTempEvent();
        event.setTime1(EMPTY);
        return event;
    }

    @Override
    public void add(PrincipalEvent value, AverageSpeedTempEvent accumulator) {
        int time1 = accumulator.getTime1();

        switch (time1) {
            case CANCEL:
                return;

            case EMPTY:

                // Starts at MIN when driving to East or MAX when driving to West.
                if ((value.getDirection() == 0 && value.getSegment() != MIN)
                    || (value.getDirection() == 1 && value.getSegment() != MAX)) {
                    accumulator.setTime1(CANCEL);
                    return;
                }

                accumulator.setInitial(
                        value.getTime(),
                        value.getPosition(),
                        value.getSegment(),
                        value.getVid(),
                        value.getHighway(),
                        value.getDirection());
                break;

            default:
                accumulator.update(
                        value.getTime(),
                        value.getPosition(),
                        value.getSegment());
                break;
        }
    }

    @Override
    public AverageSpeedEvent getResult(AverageSpeedTempEvent accumulator) {
        int time1 = accumulator.getTime1();
        if (time1 > 0) {
            // Ends at MAX when driving to East or MIN when driving to West.
            // Evaluates the average speed: m/s -> miles/h
            byte averageSpeed;
            if (accumulator.getDirection() == 0) {
                if (accumulator.getSegment2() != MAX) {
                    return ERROR;
                }
                averageSpeed = getMilesPerHour(
                        accumulator.getPosition2() - accumulator.getPosition1(),
                        accumulator.getTime2() - accumulator.getTime1());
            } else {
                if (accumulator.getSegment2() != MIN) {
                    return ERROR;
                }
                averageSpeed = getMilesPerHour(
                        accumulator.getPosition1() - accumulator.getPosition2(),
                        accumulator.getTime2() - accumulator.getTime1());
            }

            if (averageSpeed > SPEED) {
                return new AverageSpeedEvent(
                        accumulator.getTime1(),
                        accumulator.getTime2(),
                        accumulator.getVid(),
                        accumulator.getHighway(),
                        accumulator.getDirection(),
                        averageSpeed);
            }
        }
        return ERROR;
    }

    @Override
    public AverageSpeedTempEvent merge(AverageSpeedTempEvent a, AverageSpeedTempEvent b) {
        throw new Error();

        /*if (!a.isValid()) {
            return b;
        } else if (!b.isValid()) {
            return a;
        }

        int vid = a.getVid();
        int highway = a.getHighway();
        byte direction = a.getDirection();

        // Minimum
        int time1;
        int position1;
        byte segment1;
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
        byte segment2;
        if (a.getTime2() > b.getTime2()) {
            time2 = a.getTime2();
            position2 = a.getPosition2();
            segment2 = a.getSegment2();
        } else {
            time2 = b.getTime2();
            position2 = b.getPosition2();
            segment2 = b.getSegment2();
        }

        return new AverageSpeedTempEvent(
                time1, time2,
                vid, highway, direction,
                position1, position2,
                segment1, segment2);*/
    }
}
