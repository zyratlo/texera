package edu.uci.ics.textdb.dataflow.utils;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.TestConstants;

/**
 * @author sandeepreddy602
 */
public class TestUtils {

    public static boolean contains(List<ITuple> sampleTuples, ITuple actualTuple) {
        boolean contains = false;
        int schemaSize = TestConstants.SAMPLE_SCHEMA_PEOPLE.size();
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
    
    public static boolean contains(ArrayList<String> Dictionary, String returnedString) {
        boolean contains = false;
        
        for (String dictItem : Dictionary) {
        		if(dictItem.equals(returnedString))
        		{
        			contains = true;
        			return contains;
        		}
            }
            
        return contains;
    }
}
