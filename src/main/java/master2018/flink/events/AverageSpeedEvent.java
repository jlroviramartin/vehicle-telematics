/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Time1 Time2 VID XWay Dir AvgSpd
 */
public final class AverageSpeedEvent
        extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {

    public AverageSpeedEvent() {
    }

    public AverageSpeedEvent(int time1, int time2, int vid, int xway, int dir, int averageSpeed) {
        this.f0 = time1;
        this.f1 = time2;
        this.f2 = vid;
        this.f3 = xway;
        this.f4 = dir;
        this.f5 = averageSpeed;
    }

    public int getTime1() {
        return this.f0;
    }

    public int getTime2() {
        return this.f1;
    }

    public int getVID() {
        return this.f2;
    }

    public int getXWay() {
        return this.f3;
    }

    public int getDirection() {
        return this.f4;
    }

    public int getAverageSpeed() {
        return this.f5;
    }
}
