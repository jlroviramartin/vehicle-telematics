package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AverageSpeedBetweenSegmentsFilter;
import master2018.flink.functions.PrincipalEventKeySelector;
import master2018.flink.functions.PrincipalEventTimestampExtractor;
import master2018.flink.functions.AverageSpeedWindowFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSpeedReporter {

    // Evaluates the speed fines.
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .filter(new AverageSpeedBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .keyBy(new PrincipalEventKeySelector())
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new AverageSpeedWindowFunction());

        // <----- AverageSpeedWindowFunction: faltan comprobaciones y utilizar AverageSpeedEvent
        //        en vez de AverageSpeedTmpEvent
        //        Tiene un rendimiento malo, pero creo que podria funcionar.
        //        Otra solucion: SlidingWindow?
        //                       reduce?


    }

}
