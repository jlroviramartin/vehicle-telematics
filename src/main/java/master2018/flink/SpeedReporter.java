package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import master2018.flink.events.SpeedEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedReporter {

    /**
     * Speed limit used for calculating fines.
     */
    private static final int SPEED_LIMIT = 90;


    public static SingleOutputStreamOperator<SpeedEvent> analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .map(new MapFunction<PrincipalEvent, SpeedEvent>() {
                    @Override
                    public SpeedEvent map(PrincipalEvent t) throws Exception {

                        return new SpeedEvent(t.getTime(), t.getVid(),
                                t.getHighway(), t.getSegment(), t.getDirection(), t.getSpeed());
                    }
                })
                .filter(new FilterFunction<SpeedEvent>() {
                    @Override
                    public boolean filter(SpeedEvent speedEvent) throws Exception {
                        return speedEvent.getSpeed() > SPEED_LIMIT;
                    }
                });
    }

}
