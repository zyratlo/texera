package edu.uci.ics.textdb.exp.twitter;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.uci.ics.textdb.exp.common.PredicateBase;

public class TwitterConverterPredicate extends PredicateBase {
        
    @JsonCreator
    public TwitterConverterPredicate() { }

    @Override
    public TwitterConverter newOperator() {
        return new TwitterConverter();
    }

}