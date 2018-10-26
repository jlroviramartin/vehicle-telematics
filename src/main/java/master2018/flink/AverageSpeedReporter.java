package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.PrincipalEventBetweenSegmentsFilter;
import master2018.flink.functions.PrincipalEventKeySelector;
import master2018.flink.functions.PrincipalEventTimestampExtractor;
import master2018.flink.functions.PrincipalEventWindowFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSpeedReporter {

    // Evaluates the speed fines.
    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .filter(new PrincipalEventBetweenSegmentsFilter())
                .assignTimestampsAndWatermarks(new PrincipalEventTimestampExtractor())
                .keyBy(new PrincipalEventKeySelector())
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new PrincipalEventWindowFunction());

        // <----- PrincipalEventWindowFunction: faltan comprobaciones y utilizar AverageSpeedEvent
        //        en vez de AverageSpeedTmpEvent
        //        Tiene un rendimiento malo, pero creo que podria funcionar.
        //        Otra solucion: SlidingWindow?
        //                       reduce?


        /*.keyBy(1, 3, 5);
        .reduce(
        new ReduceFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
        @Override
        public Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value1,
        Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value2)
        throws Exception {
        return new Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
        value1.f0, value1.f1, value1.f2, value1.f3, value1.f4, value1.f5, value1.f6, value1.f7, value1.f8 + value2.f8);
        }
        });*/
    }

}
