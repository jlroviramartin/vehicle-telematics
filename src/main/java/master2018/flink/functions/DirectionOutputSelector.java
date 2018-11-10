package master2018.flink.functions;

import java.util.Arrays;
import java.util.List;
import master2018.flink.events.PrincipalEvent;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

/**
 * This class selects {@code PrincipalEvent}s based on the direction. It is only used in case of splitting the original
 * data ({@code tuples} or list of {@code PrincipalEvent})
 */
public final class DirectionOutputSelector implements OutputSelector<PrincipalEvent> {

    public static final String DIRECTION_0 = "direction0";
    public static final String DIRECTION_1 = "direction1";

    public DirectionOutputSelector() {
    }

    private static final List<String> PARTITION_DIR_0 = Arrays.asList(DIRECTION_0);
    private static final List<String> PARTITION_DIR_1 = Arrays.asList(DIRECTION_1);

    @Override
    public Iterable<String> select(PrincipalEvent event) {
        if (event.getDirection() == 0) {
            return PARTITION_DIR_0;
        } else {
            return PARTITION_DIR_1;
        }
    }
}
