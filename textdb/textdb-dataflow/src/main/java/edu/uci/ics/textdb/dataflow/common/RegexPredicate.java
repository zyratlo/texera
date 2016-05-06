/**
 * 
 */
package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;

/**
 * @author sandeepreddy602
 *
 */
public class RegexPredicate implements IPredicate{

    private final String fieldName;
    
    private final String regex;
    private final Pattern pattern;
    private Matcher matcher;

	public RegexPredicate(String regex, String fieldName){
        this.regex = regex;
        this.fieldName = fieldName;
        pattern = Pattern.compile(regex);
    }
	
    public String getFieldName() {
		return fieldName;
	}
    
    public String getRegex() {
    	return regex;
    }

    @Override
    public boolean satisfy(ITuple tuple) {
        if(tuple == null){
            return false;
        }
        IField field = tuple.getField(fieldName);
        if(field instanceof StringField){
            String fieldValue = ((StringField) field).getValue();
            if(fieldValue != null && fieldValue.matches(regex)){
                return true;
            }
        }
        return false;
    }
    
    /**
     * This function returns a list of spans in the given tuple that match the regex 
     * For example, given tuple ("george watson", "graduate student", 23, "(949)888-8888")
     * and regex "g[^\s]*", this function will return 
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8, "g[^\s]*", "graduate student")]
     * 
     * @param tuple document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence in the document
     */
    public List<Span> computeMatches(ITuple tuple) {
    	List<Span> spanList = new ArrayList<>();
    	if (tuple == null) {
    		return spanList; //empty array
    	}
    	IField field = tuple.getField(fieldName);
    	if (field instanceof StringField || field instanceof TextField) {
    		String fieldValue = ((StringField) field).getValue();
    		if (fieldValue == null) {
    			return spanList;
    		} else {
    			matcher = pattern.matcher(fieldValue);
    			while (matcher.find()) {
    				spanList.add(new Span(fieldName, matcher.start(), matcher.end(), regex, fieldValue));
    			}
    		}
    	}
    	
    	return spanList;
    }

}
