package edu.uci.ics.textdb.exp.comparablematcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.exp.common.PredicateBase;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

/**
 *
 * @author Adrian Seungjin Lee
 * @author Zuozhi Wang
 */
public class ComparablePredicate extends PredicateBase {

    private String attributeName;
    private Object compareToValue;
    private ComparisonType matchingType;

    @JsonCreator
    public ComparablePredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attributeName,
            @JsonProperty(value = PropertyNameConstants.COMPARE_TYPE, required = true)
            ComparisonType matchingType,
            @JsonProperty(value = PropertyNameConstants.COMPARE_TO_VALUE, required = true)
            Object compareToValue) {
        this.compareToValue = compareToValue;
        this.attributeName = attributeName;
        this.matchingType = matchingType;
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttributeName() {
        return attributeName;
    }

    @JsonProperty(value = PropertyNameConstants.COMPARE_TYPE)
    public ComparisonType getComparisonType() {
        return matchingType;
    }

    @JsonProperty(value = PropertyNameConstants.COMPARE_TO_VALUE)
    public Object getCompareToValue() {
        return compareToValue;
    }

    @Override
    public IOperator newOperator() {
        return new ComparableMatcher(this);
    }

}
