package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * This class represents a vehicle that drives too fast. It is used in the {@code SpeedReporter}.
 * <p>
 * time (f0), vid (f1), highway (f2), segment (f3), direction (f4), speed (f5)
 */
public final class SpeedEvent
        extends Tuple6<Integer, Integer, Integer, Byte, Byte, Byte> {

    public SpeedEvent() {
    }

    public SpeedEvent(int time, int vid, int highway, byte segment, byte direction, byte speed) {
        setTime(time);
        setVid(vid);
        setHighway(highway);
        setSegment(segment);
        setDirection(direction);
        setSpeed(speed);
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

    public byte getSegment() {
        return f3;
    }

    public void setSegment(byte segment) {
        f3 = segment;
    }

    public byte getDirection() {
        return f4;
    }

    public void setDirection(byte direction) {
        f4 = direction;
    }

    public byte getSpeed() {
        return f5;
    }

    public void setSpeed(byte speed) {
        f5 = speed;
    }
}
