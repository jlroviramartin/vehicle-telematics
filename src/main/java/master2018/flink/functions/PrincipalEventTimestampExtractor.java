/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.functions;

import events.events.PrincipalEvent;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * This class extracts the time in milliseconds from {@code PrincipalEvent}. It is used to assignTimestampsAndWatermarks.
 */
public final class PrincipalEventTimestampExtractor extends AscendingTimestampExtractor<PrincipalEvent> {

    public final static int MILLISECONDS = 1000;

    @Override
    public long extractAscendingTimestamp(PrincipalEvent element) {
        int newTime = element.getTime() * MILLISECONDS; // milliseconds
        return newTime;
    }
}
