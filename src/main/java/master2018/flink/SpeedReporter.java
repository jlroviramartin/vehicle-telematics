package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import master2018.flink.events.SpeedEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * This class evaluates the speed fines.
 */
public final class SpeedReporter {

    public static SingleOutputStreamOperator<SpeedEvent> analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        // Filter + Map is better...
        return tuples
                .filter(new SpeedFilterFunction())
                .map(new MapToSpeedEvent());
    }

    /**
     * This {@code MapFunction} maps from {@code PrincipalEvent} to {@code SpeedEvent}.
     */
    @FunctionAnnotation.ForwardedFields("f0; f1; f3->f2; f6->f3; f5->f4; f2->f5")
    private static final class MapToSpeedEvent implements MapFunction<PrincipalEvent, SpeedEvent> {

        @Override
        public SpeedEvent map(PrincipalEvent value) throws Exception {

            return new SpeedEvent(value.getTime(), // 0
                                  value.getVid(), // 1
                                  value.getHighway(), // 3->2
                                  value.getSegment(), //  6->3
                                  value.getDirection(), // 5->4
                                  value.getSpeed()); // 2->5
        }
    }

    /**
     * This class filters vehicles that drive faster then 90mph.
     */
    private static final class SpeedFilterFunction implements FilterFunction<PrincipalEvent> {

        /**
         * Speed limit used for calculating fines.
         */
        private static final byte SPEED_LIMIT = 90;

        public SpeedFilterFunction() {
        }

        @Override
        public boolean filter(PrincipalEvent principalEvent) throws Exception {
            return principalEvent.getSpeed() > SPEED_LIMIT;
        }
    }
}
