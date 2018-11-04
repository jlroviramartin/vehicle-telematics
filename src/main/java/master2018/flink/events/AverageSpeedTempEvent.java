/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.events;

import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Time1 (0), Time2 (1), VID (2), XWay (3), Dir (4), Pos1 (5), Pos2 (6), Seg1 (7), Seg2 (8)
 */
public final class AverageSpeedTempEvent extends Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {

    public AverageSpeedTempEvent() {
        this(-1, 0, 0, 0, 0, 0, 0, 0, 0);
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
        return this.getTime1() >= 0;
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
