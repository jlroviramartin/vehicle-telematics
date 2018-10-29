/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple9;

/**
 * ----- PRUEBA ----- Time1 (0), Time2 (1), VID (2), XWay (3), Dir (4), Pos1 (5), Pos2 (6), Seg1 (7), Seg2 (8)
 */
public final class AverageSpeedTempEvent extends Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public AverageSpeedTempEvent() {
        this.f0 = -1;
        this.f1 = 0;
        this.f2 = 0;
        this.f3 = 0;
        this.f4 = 0;
        this.f5 = 0;
        this.f6 = 0;
        this.f7 = 0;
        this.f8 = 0;
    }

    public AverageSpeedTempEvent(PrincipalEvent value) {
        this.f0 = value.getTime();
        this.f1 = value.getTime();
        this.f2 = value.getVid();
        this.f3 = value.getHighway();
        this.f4 = value.getDirection();
        this.f5 = value.getPosition();
        this.f6 = value.getPosition();
        this.f7 = value.getSegment();
        this.f8 = value.getSegment();
    }

    public AverageSpeedTempEvent(int time1, int time2, int vid, int highway, int direction, int position1, int position2, int segment1, int segment2) {
        this.f0 = time1;
        this.f1 = time2;
        this.f2 = vid;
        this.f3 = highway;
        this.f4 = direction;
        this.f5 = position1;
        this.f6 = position2;
        this.f7 = segment1;
        this.f8 = segment2;
    }

    public boolean isValid() {
        return this.getTime1() >= 0;
    }

    public int getTime1() {
        return this.f0;
    }

    public int getTime2() {
        return this.f1;
    }

    public int getVid() {
        return this.f2;
    }

    public int getHighway() {
        return this.f3;
    }

    public int getDirection() {
        return this.f4;
    }

    public int getPosition1() {
        return this.f5;
    }

    public int getPosition2() {
        return this.f6;
    }

    public int getSegment1() {
        return this.f7;
    }

    public int getSegment2() {
        return this.f8;
    }

    public void setTime1(int time1) {
        this.f0 = time1;
    }

    public void setTime2(int time2) {
        this.f1 = time2;
    }

    public void setVid(int vid) {
        this.f2 = vid;
    }

    public void setHighway(int highway) {
        this.f3 = highway;
    }

    public void setDirection(int direction) {
        this.f4 = direction;
    }

    public void setPosition1(int position1) {
        this.f5 = position1;
    }

    public void setPosition2(int position2) {
        this.f6 = position2;
    }

    public void setSegment1(int segment1) {
        this.f7 = segment1;
    }

    public void setSegment2(int segment2) {
        this.f8 = segment2;
    }
}
