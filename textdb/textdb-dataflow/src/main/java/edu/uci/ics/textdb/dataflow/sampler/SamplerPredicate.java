package edu.uci.ics.textdb.dataflow.sampler;

import edu.uci.ics.textdb.api.dataflow.IPredicate;

/**
 * @author Qinhua Huang
 *
 */
public class SamplerPredicate implements IPredicate {
    
    
    private int reservoirSize;
    SamplerPredicate(int size){
        reservoirSize = size;
    }
    
    public double getReservoirSize() {
        return reservoirSize;
    }

}