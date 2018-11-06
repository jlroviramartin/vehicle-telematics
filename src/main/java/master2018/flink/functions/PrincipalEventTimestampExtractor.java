package master2018.flink.functions;

import master2018.flink.events.PrincipalEvent;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * This class extracts the time in milliseconds of {@code PrincipalEvent}. It is used to assignTimestampsAndWatermarks.
 */
public final class PrincipalEventTimestampExtractor extends AscendingTimestampExtractor<PrincipalEvent> {

    private final static int MILLISECONDS = 1000;

    @Override
    public long extractAscendingTimestamp(PrincipalEvent element) {
        return element.getTime() * MILLISECONDS; // milliseconds
    }
}
