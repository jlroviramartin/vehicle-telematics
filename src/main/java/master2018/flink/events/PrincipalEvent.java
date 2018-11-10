package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple8;

/**
 * This class represents an event that arrives to the system.
 * <p>
 * time (f0), vid (f1), speed (f2), highway (f3), lane (f4), direction (f5), segment (f6), position (f7)
 */
public final class PrincipalEvent
        extends Tuple8<Integer, Integer, Byte, Integer, Byte, Byte, Byte, Integer> {

    public static final int VID = 1;
    public static final int HIGHWAY = 3;
    public static final int DIRECTION = 5;
    public static final int SEGMENT = 6;
    public static final int POSITION = 7;

    public PrincipalEvent() {
    }

    public PrincipalEvent(int time, int vid, byte speed, int highway, byte lane, byte direction, byte segment, int position) {
        setTime(time);
        setVid(vid);
        setSpeed(speed);
        setHighway(highway);
        setLane(lane);
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

    public byte getSpeed() {
        return f2;
    }

    public void setSpeed(byte speed) {
        f2 = speed;
    }

    public byte getLane() {
        return f4;
    }

    public void setLane(byte lane) {
        f4 = lane;
    }

    public int getHighway() {
        return f3;
    }

    public void setHighway(int highway) {
        f3 = highway;
    }

    public byte getDirection() {
        return f5;
    }

    public void setDirection(byte direction) {
        f5 = direction;
    }

    public byte getSegment() {
        return f6;
    }

    public void setSegment(byte segment) {
        f6 = segment;
    }

    public int getPosition() {
        return f7;
    }

    public void setPosition(int position) {
        f7 = position;
    }

    public void set(PrincipalEvent ev) {
        setTime(ev.getTime());
        setVid(ev.getVid());
        setSpeed(ev.getSpeed());
        setLane(ev.getLane());
        setHighway(ev.getHighway());
        setDirection(ev.getDirection());
        setSegment(ev.getSegment());
        setPosition(ev.getPosition());
    }
}
