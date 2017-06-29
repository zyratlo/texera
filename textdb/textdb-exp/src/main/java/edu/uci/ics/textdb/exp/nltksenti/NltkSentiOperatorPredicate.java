package edu.uci.ics.textdb.exp.nltksenti;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class NltkSentiOperatorPredicate extends PredicateBase {
    
    private final String inputAttributeName;
    private final String resultAttributeName;
    
    @JsonCreator
    public NltkSentiOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName) {
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
    };
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }
    
    @Override
    public NltkSentiOperator newOperator() {
        return new NltkSentiOperator(this);
    }

}
