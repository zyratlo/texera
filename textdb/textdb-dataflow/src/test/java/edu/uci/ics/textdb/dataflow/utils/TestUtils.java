package edu.uci.ics.textdb.dataflow.utils;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;

/**
 * @author sandeepreddy602
 * @author zuozhi
 */
public class TestUtils {

    public static boolean contains(List<ITuple> sampleTuples, ITuple actualTuple, List<Attribute> schema) {
        boolean contains = false;
        int schemaSize = schema.size();
        for (ITuple sampleTuple : sampleTuples) {
            contains = true;
            for (int i = 0; i < schemaSize; i++) {
                if(!sampleTuple.getField(i).equals(actualTuple.getField(i))){
                    contains = false;
                }
            }
            if(contains){
                return contains;
            }
        }
        return contains;
    }
    
    
    public static boolean equalTo(List<ITuple> tuples1, List<ITuple> tuples2) {
    	if (tuples1.size() != tuples2.size()) {
    		return false;
    	}
    	for (int i = 0; i < tuples1.size(); ++i) {
    		if (! equalTo(tuples1.get(i), tuples2.get(i))) {
    			return false;
    		}
    	}
    	return true;
    }
    
    
    public static boolean equalTo(ITuple tuple1, ITuple tuple2) {
    	List<IField> fields1 = tuple1.getFields();
    	List<IField> fields2 = tuple2.getFields();
    	if (fields1.size() != fields2.size()) {
    		return false;
    	}
    	for (int i = 0; i < fields1.size(); ++i) {
    		if (! equalTo(fields1.get(i), fields2.get(i))) {
    			return false;
    		}
    	}
    	return true;
    }
    
    
    public static boolean equalTo(IField field1, IField field2) {
    	return field1.getValue().equals(field2.getValue());
    }
}
