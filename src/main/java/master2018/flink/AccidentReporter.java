package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AccidentWindowFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * This class evaluates the accidents on the highways.
 */
public final class AccidentReporter {

    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .filter(new ZeroSpeedFilterFunction()) // speed = 0
                .keyBy(PrincipalEvent.VID,
                       PrincipalEvent.HIGHWAY,
                       PrincipalEvent.DIRECTION,
                       PrincipalEvent.SEGMENT,
                       PrincipalEvent.POSITION) // key stream by vid, highway, direction, segment, position
                .countWindow(4, 1) // Slide window, start each one event
                .apply(new AccidentWindowFunction());
    }

    /**
     * This class filters vehicles that drive at 0mph.
     */
    private static final class ZeroSpeedFilterFunction implements FilterFunction<PrincipalEvent> {

        public ZeroSpeedFilterFunction() {
        }

        @Override
        public boolean filter(PrincipalEvent event) throws Exception {
            return event.getSpeed() == 0;
        }
    }
}
