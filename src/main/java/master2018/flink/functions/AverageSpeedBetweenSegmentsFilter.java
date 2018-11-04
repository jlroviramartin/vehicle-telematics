/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.functions;

import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * This class evaluates if the segment of the {@code PrincipalEvent} is between 52 and 56. NOTE: it could be done more
 * generic using variables instead of consts... Ok, if we need it...
 */
public final class AverageSpeedBetweenSegmentsFilter implements FilterFunction<PrincipalEvent> {

    public final static int MIN = 52;
    public final static int MAX = 56;

    @Override
    public boolean filter(PrincipalEvent element) throws Exception {
        int segment = element.getSegment();
        return segment >= MIN && segment <= MAX;
    }
}
