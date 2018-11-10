package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple9;

/**
 * This class represents a vehicle that drives between the segments 52 and 56. It is used in the
 * {@code AverageSpeedReporter}.
 * <p>
 * time1 (f0), time2 (f1), vid (f2), highway (f3), direction (f4), position1 (f5), position2 (6f), segment1 (f7),
 * segment2 (f8)
 */
public final class AverageSpeedTempEvent
        extends Tuple9<Integer, Integer, Integer, Integer, Byte, Integer, Integer, Byte, Byte> {

    public static final int VID = 2;
    public static final int HIGHWAY = 3;
    public static final int DIRECTION = 4;

    public AverageSpeedTempEvent() {
    }

    public AverageSpeedTempEvent(int time1, int time2, int vid, int highway, byte direction, int position1, int position2, byte segment1, byte segment2) {
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

    public void setInitial(int time, int position, byte segment, int vid, int highway, byte direction) {
        setVid(vid);
        setHighway(highway);
        setDirection(direction);

        setTime1(time);
        setPosition1(position);
        setSegment1(segment);

        setTime2(time);
        setPosition2(position);
        setSegment2(segment);
    }

    public void update(int time, int position, byte segment) {
        setTime2(time);
        setPosition2(position);
        setSegment2(segment);
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

    public byte getDirection() {
        return this.f4;
    }

    public void setDirection(byte direction) {
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

    public byte getSegment1() {
        return this.f7;
    }

    public void setSegment1(byte segment1) {
        this.f7 = segment1;
    }

    public byte getSegment2() {
        return this.f8;
    }

    public void setSegment2(byte segment2) {
        this.f8 = segment2;
    }
}
