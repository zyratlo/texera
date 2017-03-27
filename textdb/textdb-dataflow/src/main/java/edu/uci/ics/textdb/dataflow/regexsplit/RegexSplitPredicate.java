package edu.uci.ics.textdb.dataflow.regexsplit;

import edu.uci.ics.textdb.api.dataflow.IPredicate;

/**
 * @author Qinhua Huang
 *
 */
public class RegexSplitPredicate implements IPredicate {
    
    public enum SplitType {
        GROUP_LEFT,     // the regex is grouped with the text on its left
        GROUP_RIGHT,    // the regex is grouped with the text on its right
        STANDALONE      // the regex becomes a standalone tuple
    }
    
    
    private String regex;
    private String attributeToSplit;
    private SplitType splitType;
    
    
    
    public RegexSplitPredicate(String regex, String attributeToSplit, SplitType splitType){
        this.regex = regex;
        this.attributeToSplit = attributeToSplit;
        this.splitType = splitType;
    }
    
    public String getRegex() {
        return regex;
    }
    
    public String getAttributeToSplit() {
        return attributeToSplit;
    }
    
    public SplitType getSplitType() {
        return splitType;
    }

}
