package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AccidentWindowFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * This class evaluates the accidents on the highways.
 */
public final class AccidentReporter {

    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .filter(event -> event.getSpeed() == 0) // speed = 0
                .keyBy(PrincipalEvent.VID,
                       PrincipalEvent.HIGHWAY,
                       PrincipalEvent.DIRECTION,
                       PrincipalEvent.SEGMENT,
                       PrincipalEvent.POSITION) // key stream by vid, highway, direction, segment, position
                .countWindow(4, 1) // Slide window, start each one event
                .apply(new AccidentWindowFunction())
                .setParallelism(10);
    }
}
