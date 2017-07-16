package edu.uci.ics.textdb.textql.planbuilder.beans;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;

public class PassThroughPredicate extends PredicateBase {
    
    public PassThroughPredicate(String id) {
        this.setID(id);
    }

    @Override
    public IOperator newOperator() {
        throw new UnsupportedOperationException("not implemented");
    }

}
