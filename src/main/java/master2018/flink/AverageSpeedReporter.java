package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AverageSpeedReporter {


    // Evaluates the speed fines.

    public static SingleOutputStreamOperator<PrincipalEvent> analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .filter(new FilterFunction<PrincipalEvent>() {
                    @Override
                    public boolean filter(PrincipalEvent tuple) throws Exception {
                        int segment = (int) tuple.getSegment();
                        return segment >= 52 && segment <= 56;
                    }
                });
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
