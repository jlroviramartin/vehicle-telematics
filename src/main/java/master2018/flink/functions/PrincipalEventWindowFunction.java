/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.functions;

import java.util.Iterator;
import master2018.flink.events.AverageSpeedTempEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class PrincipalEventWindowFunction
        implements WindowFunction<PrincipalEvent, AverageSpeedTempEvent, PrincipalKey, TimeWindow> {

    public PrincipalEventWindowFunction() {
    }

    @Override
    public void apply(PrincipalKey key,
                      TimeWindow window,
                      Iterable<PrincipalEvent> input,
                      Collector<AverageSpeedTempEvent> out) throws Exception {
        Iterator<PrincipalEvent> iterator = input.iterator();
        if (!iterator.hasNext()) {
            return;
        }
        PrincipalEvent v = iterator.next();

        int time1 = v.getTime(), position1 = v.getPosition(), segment1 = v.getSegment();
        int time2 = v.getTime(), position2 = v.getPosition(), segment2 = v.getSegment();

        // If the events are ordered...
        while (iterator.hasNext()) {
            v = iterator.next();
            time2 = v.getTime();
            position2 = v.getPosition();
            segment2 = v.getSegment();
        }

        AverageSpeedTempEvent result = new AverageSpeedTempEvent(
                time1, time2,
                key.getVid(), key.getHighway(), key.getDirection(),
                position1, position2,
                segment1, segment2);

        out.collect(result);
    }
}
