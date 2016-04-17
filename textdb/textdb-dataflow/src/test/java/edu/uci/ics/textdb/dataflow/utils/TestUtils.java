package edu.uci.ics.textdb.dataflow.utils;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;

/**
 * @author sandeepreddy602
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
}
