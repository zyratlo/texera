package edu.uci.ics.textdb.exp.twitter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class TwitterConverterPredicate extends PredicateBase {
    
    private final String rawDataAttribute;
    
    @JsonCreator
    public TwitterConverterPredicate(
            @JsonProperty(value = PropertyNameConstants.TWITTER_RAW_ATTRIBUTE, required = true)
            String rawDataAttribute
            ) {
        String rawDataAttributeTrim = rawDataAttribute.trim();
        if (rawDataAttributeTrim.isEmpty()) {
            throw new DataFlowException("rawDataAttribute is empty");
        } else {
            this.rawDataAttribute = rawDataAttributeTrim;
        }
    }
    
    @JsonProperty(value = PropertyNameConstants.TWITTER_RAW_ATTRIBUTE)
    public String getRawDataAttribute() {
        return this.rawDataAttribute;
    }

    @Override
    public TwitterConverter newOperator() {
        return new TwitterConverter(this);
    }

}