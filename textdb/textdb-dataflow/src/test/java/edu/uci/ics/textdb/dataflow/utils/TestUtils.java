package edu.uci.ics.textdb.dataflow.utils;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.utils.Utils;

/**
 * @author sandeepreddy602
 * @author zuozhi
 * @author rajeshyarlagadda
 */
public class TestUtils {

    /**
     * Returns if the tupleList contains a tuple.
     * 
     * _ID and PAYLOAD fields are ignored in the comparison, because they are not relevant.
     * 
     * @param tupleList
     * @param containsTuple
     * @return
     */
    public static boolean contains(List<ITuple> tupleList, ITuple containsTuple) {
        tupleList = Utils.removeFields(tupleList, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        containsTuple = Utils.removeFields(containsTuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        
        return tupleList.contains(containsTuple);
    }
    
    /**
     * Returns if the tupleList contains a list of tuples.
     * 
     * _ID and PAYLOAD fields are ignored in the comparison, because they are not relevant.
     * 
     * @param tupleList
     * @param containsTupleList
     * @return
     */
    public static boolean containsAll(List<ITuple> tupleList, List<ITuple> containsTupleList) {
        tupleList = Utils.removeFields(tupleList, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        containsTupleList = Utils.removeFields(containsTupleList, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        
        return tupleList.containsAll(containsTupleList);
    }
    
    /**
     * Returns if the two tuple lists are equivalent (order doesn't matter)
     * 
     * _ID and PAYLOAD fields are ignored in the comparison, because they are not relevant.
     * 
     * @param expectedResults
     * @param exactResults
     * @return
     */
    public static boolean equals(List<ITuple> expectedResults, List<ITuple> exactResults) {
        expectedResults = Utils.removeFields(expectedResults, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        exactResults = Utils.removeFields(exactResults, SchemaConstants._ID, SchemaConstants.PAYLOAD);

        if (expectedResults.size() != exactResults.size())
            return false;
        
        return (expectedResults.containsAll(exactResults)) || (! exactResults.containsAll(expectedResults));
    }

    public static boolean checkResults(List<ITuple> results, String queryString, Analyzer queryAnalyzer,
            String searchField) throws ParseException {

        List<String> listOfQueryWords = Utils.tokenizeQuery(queryAnalyzer, queryString);

        for (ITuple sampleTuple : results) {
            String value = (String) sampleTuple.getField(searchField).getValue();
            for (String queryWord : listOfQueryWords) {
                if (value.toLowerCase().contains(queryWord.toLowerCase())) {
                    return true;
                }
            }
        }
        return false;
    }
    
}
