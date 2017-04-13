package edu.uci.ics.textdb.exp.sink.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.uci.ics.textdb.api.dataflow.IPredicate;

public class TupleSinkPredicate implements IPredicate {
    
    @JsonCreator
    public TupleSinkPredicate() {}

}
