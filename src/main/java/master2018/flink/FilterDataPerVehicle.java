/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * This class is only used for testing purposes.
 */
public class FilterDataPerVehicle {

    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples, int vid) {
        return tuples
                .filter(new FilterFunction<PrincipalEvent>() {
                    @Override
                    public boolean filter(PrincipalEvent value) throws Exception {
                        return value.getVid() == vid;
                    }
                });

    }
}
