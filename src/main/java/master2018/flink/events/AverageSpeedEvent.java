package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * This class represents a vehicle that drives too fast between the segments 52 and 56. It is used in the
 * {@code AverageSpeedReporter}.
 * <p>
 * time1 (f0), time2 (f1), vid (f2), highway (f3), direction (f4), averageSpeed (f5)
 */
public final class AverageSpeedEvent
        extends Tuple6<Integer, Integer, Integer, Integer, Byte, Double> {

    public AverageSpeedEvent() {
    }

    public AverageSpeedEvent(int time1, int time2, int vid, int highway, byte direction, double averageSpeed) {
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

    public byte getDirection() {
        return this.f4;
    }

    public void setDirection(byte direccion) {
        this.f4 = direccion;
    }

    public double getAverageSpeed() {
        return this.f5;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.f5 = averageSpeed;
    }
}
