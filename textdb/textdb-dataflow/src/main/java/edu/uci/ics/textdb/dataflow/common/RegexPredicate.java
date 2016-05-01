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

/**
 * @author sandeepreddy602
 *
 */
public class RegexPredicate implements IPredicate{

    private final String fieldName;
    private final String regex;

	public RegexPredicate(String regex, String fieldName){
        this.regex = regex;
        this.fieldName = fieldName;
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
    
    public List<Span> statisfySpan(ITuple tuple) {
    	List<Span> res = new ArrayList<>();
    	if (tuple == null) {
    		return res; //empty array
    	}
    	IField field = tuple.getField(fieldName);
    	if (field instanceof StringField) {
    		String fieldValue = ((StringField) field).getValue();
    		if (fieldValue == null) {
    			return res;
    		} else {
    			Pattern p = Pattern.compile(regex);
    			Matcher m = p.matcher(fieldValue);
    			while (m.find()) {
    				res.add(new Span(fieldName, m.start(), m.end(), regex, fieldValue));
    			}
    		}
    	}
    	
    	return res;
    }

}
