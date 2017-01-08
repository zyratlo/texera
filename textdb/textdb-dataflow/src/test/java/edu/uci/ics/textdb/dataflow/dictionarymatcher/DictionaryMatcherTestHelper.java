package edu.uci.ics.textdb.dataflow.dictionarymatcher;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.relation.RelationManager;

public class DictionaryMatcherTestHelper {
    
    public static final String PEOPLE_TABLE = "dicttest_people";
    
    public static void writeTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, "../index/dicttest/people/", 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());        
        for (ITuple tuple : TestConstants.getSamplePeopleTuples()) {
            relationManager.insertTuple(PEOPLE_TABLE, tuple);
        }
          
    }
    
    public static void deleteTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(PEOPLE_TABLE);
    }
    
    public static List<ITuple> getQueryResults(String tableName, IDictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType) throws TextDBException {
        return getQueryResults(tableName, dictionary, attributeNames, matchingType, Integer.MAX_VALUE, 0);
    }
    
    public static List<ITuple> getQueryResults(String tableName, IDictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        
        // results from a scan on the table followed by a keyword match
        List<ITuple> scanSourceResults = getScanSourceResults(tableName, dictionary, attributeNames,
                matchingType, limit, offset);
        // reset the dictionary's cursor after each run because the same dictionary may be used later.
        dictionary.resetCursor();
        // results from index-based keyword search on the table
        List<ITuple> dictionarySourceResults = getDictionarySourceResults(tableName, dictionary, attributeNames,
                matchingType, limit, offset);
        dictionary.resetCursor();
        
        // if limit and offset are not relevant, the results from scan source and keyword source must be the same
        if (limit == Integer.MAX_VALUE && offset == 0) {
            if (TestUtils.equals(scanSourceResults, dictionarySourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataFlowException("results from scanSource and dictionarySource are inconsistent");
            }
        }
        // if limit and offset are relevant, then the results can be different (since the order doesn't matter)
        // in this case, we get all the results and test if the whole result set contains both results
        else {
            List<ITuple> allResults = getDictionarySourceResults(tableName, dictionary, attributeNames,
                    matchingType, Integer.MAX_VALUE, 0);
            dictionary.resetCursor();
            
            if (scanSourceResults.size() == dictionarySourceResults.size() &&
                    TestUtils.containsAll(allResults, scanSourceResults) && 
                    TestUtils.containsAll(allResults, dictionarySourceResults)) {
                return scanSourceResults;
            } else {
                throw new DataFlowException("results from scanSource and dictionarySource are inconsistent");
            }   
        }
    }
    
    /**
     * Get the results from a DictionaryMatcher with a ScanSource Operator 
     *   (which scans the table first and then feeds the data into the dictionary matcher)
     * 
     * @param tableName
     * @param dictionary
     * @param attributeNames
     * @param matchingType
     * @param limit
     * @param offset
     * @return
     * @throws TextDBException
     */
    public static List<ITuple> getScanSourceResults(String tableName, IDictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        
        RelationManager relationManager = RelationManager.getRelationManager();
        Analyzer luceneAnalyzer = relationManager.getTableAnalyzer(tableName);
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(tableName);
        
        DictionaryPredicate dictiaonryPredicate = new DictionaryPredicate(
                dictionary, attributeNames, luceneAnalyzer, matchingType);
        DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictiaonryPredicate);
        
        dictionaryMatcher.setLimit(limit);
        dictionaryMatcher.setOffset(offset);
        
        dictionaryMatcher.setInputOperator(scanSource);
        
        ITuple tuple;
        List<ITuple> results = new ArrayList<>();
        
        dictionaryMatcher.open();
        while ((tuple = dictionaryMatcher.getNextTuple()) != null) {
            results.add(tuple);
        }  
        dictionaryMatcher.close();
        
        return results;
    }
    
    /**
     * Get the results from the DictionarySourceOperator 
     *   (which performs a index-based lookup for each keyword in the dictionary)
     * 
     * @param tableName
     * @param dictionary
     * @param attributeNames
     * @param matchingType
     * @param limit
     * @param offset
     * @return
     * @throws TextDBException
     */
    public static List<ITuple> getDictionarySourceResults(String tableName, IDictionary dictionary, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        Analyzer luceneAnalyzer = relationManager.getTableAnalyzer(tableName);
        
        DictionaryPredicate dictiaonryPredicate = new DictionaryPredicate(
                dictionary, attributeNames, luceneAnalyzer, matchingType);
        DictionaryMatcherSourceOperator dictionarySource = new DictionaryMatcherSourceOperator(
                dictiaonryPredicate, relationManager.getTableDataStore(tableName));

        dictionarySource.setLimit(limit);
        dictionarySource.setOffset(offset);
        
        ITuple tuple;
        List<ITuple> results = new ArrayList<>();
        
        dictionarySource.open();
        while ((tuple = dictionarySource.getNextTuple()) != null) {
            results.add(tuple);
        }
        dictionarySource.close();
        
        return results;
    }

}
