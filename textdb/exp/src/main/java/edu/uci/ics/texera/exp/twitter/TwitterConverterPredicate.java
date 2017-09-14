package edu.uci.ics.texera.exp.twitter;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.uci.ics.texera.exp.common.PredicateBase;

public class TwitterConverterPredicate extends PredicateBase {
        
    @JsonCreator
    public TwitterConverterPredicate() { }

    @Override
    public TwitterConverter newOperator() {
        return new TwitterConverter();
    }

}