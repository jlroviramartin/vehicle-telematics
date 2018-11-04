package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple7;

public final class AccidentEvent
        extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    // time1 - f0
    // time1 - f1
    // vid - f2
    // highway - f3
    // segment - f4
    // direction - f5
    // position - f6

    public AccidentEvent() {
    }

    public AccidentEvent(int time1, int time2, int vid, int highway, int segment, int direction, int position) {
        f0 = time1;
        f1 = time2;
        f2 = vid;
        f3 = highway;
        f4 = segment;
        f5 = direction;
        f6 = position;
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
}