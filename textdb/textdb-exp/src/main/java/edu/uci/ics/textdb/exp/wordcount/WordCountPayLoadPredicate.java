package edu.uci.ics.textdb.exp.wordcount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 */

public class WordCountPayLoadPredicate extends PredicateBase {
        
    private final String attribute;
    private final String analyzer;
    
    @JsonCreator
    public WordCountPayLoadPredicate(
            @JsonProperty(value = PropertyNameConstants.COUNT_ATTRIBUTE, required = true)
            String attribute,
            @JsonProperty(value = PropertyNameConstants.COUNT_ANALYZER, required = true)
            String analyzer) {
        
        this.attribute = attribute;
        this.analyzer = analyzer;
    }
    
    @JsonProperty(PropertyNameConstants.COUNT_ATTRIBUTE)
    public String getAttribute() {
        return this.attribute;
    }
    
    @JsonProperty(PropertyNameConstants.COUNT_ANALYZER)
    public String getLuceneAnalyzerString() {
        return this.analyzer;
    }

    @Override
    public IOperator newOperator() {
        // TODO Auto-generated method stub
        return new WordCountPayLoad(this);
    }
    
}
