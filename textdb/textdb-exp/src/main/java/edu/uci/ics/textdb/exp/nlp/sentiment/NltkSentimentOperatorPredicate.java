package edu.uci.ics.textdb.exp.nlp.sentiment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class NltkSentimentOperatorPredicate extends PredicateBase {
    
    private final String inputAttributeName;
    private final String resultAttributeName;
    private final int sizeTupleBuffer;
    
    @JsonCreator
    public NltkSentimentOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName,
            @JsonProperty(value = PropertyNameConstants.NLP_NLTK_BUFFER_SIZE, required = true)
            int sizeTupleBuffer) {
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
        this.sizeTupleBuffer = sizeTupleBuffer;
    };
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.NLP_NLTK_BUFFER_SIZE)
    public int getSizeTupleBuffer() {
        return this.sizeTupleBuffer;
    }
    
    @Override
    public NltkSentimentOperator newOperator() {
        return new NltkSentimentOperator(this);
    }

}
