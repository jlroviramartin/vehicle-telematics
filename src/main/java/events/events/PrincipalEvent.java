package events.events;

import org.apache.flink.api.java.tuple.Tuple8;

public class PrincipalEvent
        extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    // time - f0
    // vid - f1
    // speed - f2
    // highway - f3
    // lane - f4
    // direction - f5
    // segment - f6
    // position - f7

    public PrincipalEvent() {
    }

    public PrincipalEvent(int time, int vid, int speed, int highway, int lane, int direction, int segment, int position) {
        f0 = time;
        f1 = vid;
        f2 = speed;
        f3 = highway;
        f4 = lane;
        f5 = direction;
        f6 = segment;
        f7 = position;
    }

    public int getTime() {
        return f0;
    }

    public Integer getVid() {
        return f1;
    }

    public Integer getSpeed() {
        return f2;
    }

    public Integer getLane() {
        return f4;
    }

    public Integer getHighway() {
        return f3;
    }

    public Integer getDirection() {
        return f5;
    }

    public Integer getSegment() {
        return f6;
    }

    public void setTime(int time) {
        f0 = time;
    }

    public void setVid(Integer vid) {
        f1 = vid;
    }

    public void setSpeed(int speed) {
        f2 = speed;
    }

    public void setHighway(int highway) {
        f3 = highway;
    }

    public void setLane(int lane) {
        f4 = lane;
    }

    public void setDirection(int direction) {
        f5 = direction;
    }

    public void setSegment(int segment) {
        f6 = segment;
    }

    public void setPosition(int position) {
        f7 = position;
    }

}