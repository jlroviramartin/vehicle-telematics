/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.functions;

import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * This class parses a string into a {@code PrincipalEvent}.
 */
public final class ParsePrincipalEventMapFunction implements MapFunction<String, PrincipalEvent> {

    public ParsePrincipalEventMapFunction() {
    }

    @Override
    public PrincipalEvent map(String in) throws Exception {

        String[] split = in.split(",");

        if (split.length < 8) {
            throw new Exception("This line cannot be splitted: " + in);
        }

        return new PrincipalEvent(
                Integer.parseInt(split[0].trim()),
                Integer.parseInt(split[1].trim()),
                Byte.parseByte(split[2].trim()),
                Integer.parseInt(split[3].trim()),
                Byte.parseByte(split[4].trim()),
                Byte.parseByte(split[5].trim()),
                Byte.parseByte(split[6].trim()),
                Integer.parseInt(split[7].trim()));
    }
}
