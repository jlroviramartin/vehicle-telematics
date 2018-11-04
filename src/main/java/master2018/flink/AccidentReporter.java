package master2018.flink;

import master2018.flink.events.PrincipalEvent;
import master2018.flink.functions.AccidentWindowFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AccidentReporter {

    private static final int VID = 1;
    private static final int HIGHWAY = 3;
    private static final int DIRECTION = 5;
    private static final int SEGMENT = 6;
    private static final int POSITION = 7;


    public static SingleOutputStreamOperator analyze(SingleOutputStreamOperator<PrincipalEvent> tuples) {

        return tuples
                .filter(event -> event.getSpeed() == 0) // speed = 0
                .keyBy(VID, HIGHWAY, DIRECTION, SEGMENT, POSITION) // key stream by vid, highway, direction, segment, position
                .countWindow(4, 1) // Slide window, start each one event
                .apply(new AccidentWindowFunction());
    }

}