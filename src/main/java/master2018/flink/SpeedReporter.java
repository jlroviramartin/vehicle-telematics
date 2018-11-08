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

    /**
     * Speed limit used for calculating fines.
     */
    private static final int SPEED_LIMIT = 90;

    public static SingleOutputStreamOperator<SpeedEvent> analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .map(new MapToSpeedEvent())
                .filter(new FilterFunction<SpeedEvent>() {
                    @Override
                    public boolean filter(SpeedEvent speedEvent) throws Exception {
                        return speedEvent.getSpeed() > SPEED_LIMIT;
                    }
                });
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
}
