package edu.uci.ics.textdb.dataflow.regexsplit;

import java.util.List;

import edu.uci.ics.textdb.api.common.IPredicate;

/**
 * @author Qinhua Huang
 *
 */
public class RegexSplitPredicate implements IPredicate {
    private List<String> attributeNames;
    private String regex;
    private String attributeToSplit;
    
    
    
    public RegexSplitPredicate(String regex, List<String> attributeNames, String attributeToSplit){
        this.regex = regex;
 //       this.attributeNames = attributeNames;
        this.attributeToSplit = attributeToSplit;
    }
    
    public String getRegex() {
        return regex;
    }
    
    public String getAttributeToSplit() {
        return attributeToSplit;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

}
