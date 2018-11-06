package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Time1 Time2 VID XWay Dir AvgSpd
 */
public final class AverageSpeedEvent
        extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {

    public AverageSpeedEvent() {
    }

    public AverageSpeedEvent(int time1, int time2, int vid, int highway, int direction, int averageSpeed) {
        setTime1(time1);
        setTime2(time2);
        setVID(vid);
        setHighway(highway);
        setDirection(direction);
        setAverageSpeed(averageSpeed);
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

    public int getVID() {
        return this.f2;
    }

    public void setVID(int vid) {
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

    public void setDirection(int direccion) {
        this.f4 = direccion;
    }

    public int getAverageSpeed() {
        return this.f5;
    }

    public void setAverageSpeed(int averageSpeed) {
        this.f5 = averageSpeed;
    }
}
