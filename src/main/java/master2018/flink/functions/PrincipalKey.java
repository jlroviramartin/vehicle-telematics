/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.functions;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Key for PrincipalEvent.
 */
public class PrincipalKey extends Tuple3<Integer, Integer, Integer> {

    public PrincipalKey() {
    }

    public PrincipalKey(int vid, int highway, int direction) {
        f0 = vid;
        f1 = highway;
        f2 = direction;
    }

    public int getVid() {
        return f0;
    }

    public int getHighway() {
        return f1;
    }

    public int getDirection() {
        return f2;
    }
}
