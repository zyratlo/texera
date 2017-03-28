package edu.uci.ics.textdb.exp.sampler;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 *
 */
public class SamplerPredicate implements IPredicate {
    
    
    private final int reservoirSize;
    SamplerPredicate(
            @JsonProperty(value = PropertyNameConstants.RESERVOIR_SIZE, required = true)
            int reservoirSize ) {
        this.reservoirSize = reservoirSize;
    }
    
    @JsonProperty(PropertyNameConstants.RESERVOIR_SIZE)
    public double getReservoirSize() {
        return reservoirSize;
    }

}