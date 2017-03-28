package edu.uci.ics.textdb.exp.regexsplit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 *
 */
public class RegexSplitPredicate implements IPredicate {
    
    public enum SplitType {
        GROUP_LEFT("left"), // the regex is grouped with the text on its left

        GROUP_RIGHT("right"), // the regex is grouped with the text on its right

        STANDALONE("standalone"); // the regex becomes a standalone tuple
        
        private final String name;
        
        private SplitType(String name) {
            this.name = name;
        }
        
        // use the name string instead of enum string in JSON
        @JsonValue
        public String getName() {
            return this.name;
        }
    }
    

    
    private final String regex;
    private final String attributeToSplit;
    private final SplitType splitType;
    
    /**
     * Construct a RegexSplitPredicate.
     * 
     * @param regex, the regex query
     * @param attributeToSplit, the attribute name to perform split operation on
     * @param splitType, a type to indicate where the regex pattern merge into. 
     */
    @JsonCreator
    public RegexSplitPredicate(
            @JsonProperty(value=PropertyNameConstants.REGEX_QUERY, required=true)
            String regex,            
            @JsonProperty(value=PropertyNameConstants.ATTRIBUTE_TO_SPLIT, required=true)
            String attributeToSplit,            
            @JsonProperty(value=PropertyNameConstants.SPLIT_TYPE, required=true)
            SplitType splitType ) {
        this.regex = regex;
        this.attributeToSplit = attributeToSplit;
        this.splitType = splitType;
    }
    
    @JsonProperty(PropertyNameConstants.REGEX_QUERY)
    public String getRegex() {
        return regex;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_TO_SPLIT)
    public String getAttributeToSplit() {
        return attributeToSplit;
    }
    
    @JsonProperty(PropertyNameConstants.SPLIT_TYPE)
    public SplitType getSplitType() {
        return splitType;
    }

}
