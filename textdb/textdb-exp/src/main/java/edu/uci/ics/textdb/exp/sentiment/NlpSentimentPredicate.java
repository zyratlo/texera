package edu.uci.ics.textdb.exp.sentiment;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class NlpSentimentPredicate implements IPredicate {
    
    private final String targetAttributeName;
    private final String resultAttributeName;
    
    public NlpSentimentPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String targetAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName
            ) {
        this.targetAttributeName = targetAttributeName;
        this.resultAttributeName = resultAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getTargetAttributeName() {
        return this.targetAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }

}
