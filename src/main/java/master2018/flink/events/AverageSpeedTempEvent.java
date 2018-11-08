package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple9;

/**
 * This class represents a vehicle that drives between the segments 52 and 56. It is used in the
 * {@code AverageSpeedReporter}.
 * <p>
 * time1 (f0), time2 (f1), vid (f2), highway (f3), direction (f4), position1 (f5), position2 (6f), segment1 (f7), segment2 (f8)
 */
public final class AverageSpeedTempEvent
        extends Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public static final int VID = 2;
    public static final int HIGHWAY = 3;
    public static final int DIRECTION = 4;

    public AverageSpeedTempEvent() {
    }

    public AverageSpeedTempEvent(PrincipalEvent value) {
        this(value.getTime(),
             value.getTime(),
             value.getVid(),
             value.getHighway(),
             value.getDirection(),
             value.getPosition(),
             value.getPosition(),
             value.getSegment(),
             value.getSegment());
    }

    public AverageSpeedTempEvent(int time1, int time2, int vid, int highway, int direction, int position1, int position2, int segment1, int segment2) {
        setTime1(time1);
        setTime2(time2);
        setVid(vid);
        setHighway(highway);
        setDirection(direction);
        setPosition1(position1);
        setPosition2(position2);
        setSegment1(segment1);
        setSegment2(segment2);
    }

    public boolean isValid() {
        return this.f0 != null;
    }

    public int getTime1() {
        return this.f0;
    }

    public void setTime1(int time1) {
        this.f0 = time1;
    }

    public int getTime2() {
        return this.f1;
    }

    public void setTime2(int time2) {
        this.f1 = time2;
    }

    public int getVid() {
        return this.f2;
    }

    public void setVid(int vid) {
        this.f2 = vid;
    }

    public int getHighway() {
        return this.f3;
    }

    public void setHighway(int highway) {
        this.f3 = highway;
    }

    public int getDirection() {
        return this.f4;
    }

    public void setDirection(int direction) {
        this.f4 = direction;
    }

    public int getPosition1() {
        return this.f5;
    }

    public void setPosition1(int position1) {
        this.f5 = position1;
    }

    public int getPosition2() {
        return this.f6;
    }

    public void setPosition2(int position2) {
        this.f6 = position2;
    }

    public int getSegment1() {
        return this.f7;
    }

    public void setSegment1(int segment1) {
        this.f7 = segment1;
    }

    public int getSegment2() {
        return this.f8;
    }

    public void setSegment2(int segment2) {
        this.f8 = segment2;
    }
}
