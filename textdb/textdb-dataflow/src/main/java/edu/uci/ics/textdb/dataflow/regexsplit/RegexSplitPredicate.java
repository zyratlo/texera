package edu.uci.ics.textdb.dataflow.regexsplit;

import edu.uci.ics.textdb.api.common.IPredicate;

/**
 * @author Qinhua Huang
 *
 */
public class RegexSplitPredicate implements IPredicate {
    private String regex;
    private String attributeToSplit;
    
    
    
    public RegexSplitPredicate(String regex, String attributeToSplit){
        this.regex = regex;
        this.attributeToSplit = attributeToSplit;
    }
    
    public String getRegex() {
        return regex;
    }
    
    public String getAttributeToSplit() {
        return attributeToSplit;
    }

}
