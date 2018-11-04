package master2018.flink.functions;

import java.util.Iterator;
import master2018.flink.Utils;
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

        if (Utils.size(iterable) == 4) {

            Iterator<PrincipalEvent> events = iterable.iterator();

            PrincipalEvent firstEvent = events.next();
            int time1 = firstEvent.getTime();

            PrincipalEvent lastEvent = firstEvent; // Avoid "can be null"
            while (events.hasNext()) {
                lastEvent = events.next();
            }

            AccidentEvent accidentEvent = new AccidentEvent();
            accidentEvent.setTime1(time1);
            accidentEvent.setTime2(lastEvent.getTime());
            accidentEvent.setVid(firstEvent.getVid());
            accidentEvent.setHighway(firstEvent.getHighway());
            accidentEvent.setSegment(firstEvent.getSegment());
            accidentEvent.setDirection(firstEvent.getDirection());
            accidentEvent.setPosition(firstEvent.getPosition());

            accidents.collect(accidentEvent);
        }
    }
}