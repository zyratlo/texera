package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;
import edu.uci.ics.textdb.storage.relation.RelationManager;

public class KeywordTestHelper {
    
    public static final String PEOPLE_TABLE = "keywordtest_people";
    public static final String MEDLINE_TABLE = "keywordtest_medline";
    
    public static void writeTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the people table and write tuples
        relationManager.createTable(PEOPLE_TABLE, "../index/keywordtest/people/", 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());        
        for (ITuple tuple : TestConstants.getSamplePeopleTuples()) {
            relationManager.insertTuple(PEOPLE_TABLE, tuple);
        }
        
        // create the medline table and write tuples
        relationManager.createTable(MEDLINE_TABLE, "../index/keywordtest/medline/",
                keywordTestConstants.SCHEMA_MEDLINE, LuceneAnalyzerConstants.standardAnalyzerString());       
        for (ITuple tuple : keywordTestConstants.getSampleMedlineRecord()) {
            relationManager.insertTuple(MEDLINE_TABLE, tuple);
        }       
    }
    
    public static void deleteTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();

        relationManager.deleteTable(PEOPLE_TABLE);
        relationManager.deleteTable(MEDLINE_TABLE);
    }
    
    public static List<ITuple> getQueryResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType) throws TextDBException {
        return getQueryResults(tableName, keywordQuery, attributeNames, matchingType, Integer.MAX_VALUE, 0);
    }
    
    public static List<ITuple> getQueryResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        
        List<ITuple> scanSourceResults = getScanSourceResults(tableName, keywordQuery, attributeNames,
                matchingType, limit, offset);
        List<ITuple> keywordSourceResults = getKeywordSourceResults(tableName, keywordQuery, attributeNames,
                matchingType, limit, offset);
        
        if (limit == Integer.MAX_VALUE && offset == 0) {
            if (! TestUtils.equals(scanSourceResults, keywordSourceResults)) {
                throw new DataFlowException("results from scanSource and keywordSource are not the same");
            } else {
                return scanSourceResults;
            }
        }
        
        return null;
    }
    
    public static List<ITuple> getScanSourceResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(tableName);
        
        KeywordPredicate keywordPredicate = new KeywordPredicate(
                keywordQuery, attributeNames, LuceneAnalyzerConstants.getStandardAnalyzer(), matchingType);
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
        keywordMatcher.setLimit(limit);
        keywordMatcher.setOffset(offset);
        
        keywordMatcher.setInputOperator(scanSource);
        
        ITuple tuple;
        List<ITuple> results = new ArrayList<>();
        
        keywordMatcher.open();
        while ((tuple = keywordMatcher.getNextTuple()) != null) {
            results.add(tuple);
        }  
        keywordMatcher.close();
        
        return results;
    }
    
    public static List<ITuple> getKeywordSourceResults(String tableName, String keywordQuery, List<String> attributeNames,
            KeywordMatchingType matchingType, int limit, int offset) throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        KeywordPredicate keywordPredicate = new KeywordPredicate(
                keywordQuery, attributeNames, LuceneAnalyzerConstants.getStandardAnalyzer(), matchingType);
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(
                keywordPredicate, relationManager.getTableDataStore(tableName));
        keywordSource.setLimit(limit);
        keywordSource.setOffset(offset);
        
        ITuple tuple;
        List<ITuple> results = new ArrayList<>();
        
        keywordSource.open();
        while ((tuple = keywordSource.getNextTuple()) != null) {
            results.add(tuple);
        }
        keywordSource.close();
        
        return results;
    }

}
