package master2018.flink.functions;

import java.util.Iterator;
import master2018.flink.events.AccidentEvent;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public final class AccidentWindowFunction
        implements WindowFunction<PrincipalEvent, AccidentEvent, Tuple, GlobalWindow> {

    @Override
    public void apply(Tuple key, GlobalWindow globalWindow, Iterable<PrincipalEvent> iterable, Collector<AccidentEvent> accidents) {

        //if (Iterables.size(iterable) == 4) {
        Iterator<PrincipalEvent> events = iterable.iterator();

        PrincipalEvent firstEvent = events.next();
        int count = 1;
        int time = firstEvent.getTime();

        PrincipalEvent lastEvent = firstEvent; // Avoid "can be null"
        while (events.hasNext()) {
            lastEvent = events.next();
            count++;
            time += 30;

            // Early exit if the events are not ordered.
            // It could be used a boolean variable instead of an early return. This could be cleaner...
            // as you wish my lord...
            if (time != lastEvent.getTime()) {
                return;
            }
        }

        if (count != 4) {
            return;
        }

        AccidentEvent accidentEvent = new AccidentEvent(
                firstEvent.getTime(),
                lastEvent.getTime(),
                firstEvent.getVid(),
                firstEvent.getHighway(),
                firstEvent.getSegment(),
                firstEvent.getDirection(),
                firstEvent.getPosition());

        accidents.collect(accidentEvent);
    }
}
