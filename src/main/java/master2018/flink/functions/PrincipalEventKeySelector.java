/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package master2018.flink.functions;

import master2018.flink.events.PrincipalEvent;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * This class extracts the key of {@code PrincipalEvent}.
 */
public final class PrincipalEventKeySelector implements KeySelector<PrincipalEvent, PrincipalKey> {

    public PrincipalEventKeySelector() {
    }

    @Override
    public PrincipalKey getKey(PrincipalEvent principalEvent) {
        return new PrincipalKey(principalEvent.getVid(),
                                principalEvent.getHighway(),
                                principalEvent.getDirection());
    }
}
