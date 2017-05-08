package edu.uci.ics.textdb.exp.wordcount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 */

public class WordCountIndexSourcePredicate extends PredicateBase {

    private final String tableName;
    private final String attribute;
    
    @JsonCreator
    public WordCountIndexSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.COUNT_TABLE, required = true)
            String tableName,
            @JsonProperty(value = PropertyNameConstants.COUNT_ATTRIBUTE, required = true)
            String attribute) {
        
        this.tableName = tableName;
        this.attribute = attribute;
        
    }
    
    @JsonProperty(PropertyNameConstants.COUNT_TABLE)
    public String getTableName() {
        return this.tableName;
    }
    
    @JsonProperty(PropertyNameConstants.COUNT_ATTRIBUTE)
    public String getAttribute() {
        return this.attribute;
    }

    @Override
    public IOperator newOperator() {
        return new WordCountIndexSource(this);
    }
    
}
