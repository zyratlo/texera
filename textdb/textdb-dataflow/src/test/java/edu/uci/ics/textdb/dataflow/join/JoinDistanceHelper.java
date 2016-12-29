package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.relation.RelationManager;

public class JoinDistanceHelper {
    
    public static final String BOOK_TABLE_OUTER = "jointest_book_outer";
    public static final String BOOK_TABLE_INNER = "jointest_book_inner";
    
    public static void createTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create the two book tables
        relationManager.createTable(BOOK_TABLE_OUTER, "../index/jointest/book_outer/", 
                JoinTestConstants.BOOK_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());     
        relationManager.createTable(BOOK_TABLE_INNER, "../index/jointest/book_inner/", 
                JoinTestConstants.BOOK_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());         
    }
    
    public static void insertToOuter(ITuple... tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getRelationManager();
        for (int i = 0; i < tuples.length; i++) {
            relationManager.insertTuple(BOOK_TABLE_OUTER, tuples[i]);
        }
    }
    
    public static void insertToOuter(List<ITuple> tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getRelationManager();
        for (ITuple tuple: tuples) {
            relationManager.insertTuple(BOOK_TABLE_OUTER, tuple);
        }
    } 
    
    public static void insertToInner(ITuple... tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getRelationManager();
        for (int i = 0; i < tuples.length; i++) {
            relationManager.insertTuple(BOOK_TABLE_INNER, tuples[i]);
        }
    }
    
    public static void insertToInner(List<ITuple> tuples) throws StorageException {
        RelationManager relationManager = RelationManager.getRelationManager();
        for (ITuple tuple: tuples) {
            relationManager.insertTuple(BOOK_TABLE_INNER, tuple);
        }
    } 
    
    public static void clearTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTuples(BOOK_TABLE_OUTER, new MatchAllDocsQuery());
        relationManager.deleteTuples(BOOK_TABLE_INNER, new MatchAllDocsQuery());      
    }
    
    public static void deleteTestTables() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(BOOK_TABLE_OUTER);
        relationManager.deleteTable(BOOK_TABLE_INNER);
    }
    
    public static KeywordMatcherSourceOperator getKeywordSource(String tableName, String query, 
            KeywordMatchingType matchingType) throws TextDBException {
        DataStore tableDataStore = RelationManager.getRelationManager().getTableDataStore(tableName);
        KeywordPredicate keywordPredicate = new KeywordPredicate(query, 
                Arrays.asList(JoinTestConstants.AUTHOR, JoinTestConstants.TITLE, JoinTestConstants.REVIEW),
                LuceneAnalyzerConstants.getStandardAnalyzer(), matchingType);
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(keywordPredicate, tableDataStore);
        return keywordSource;
    }
    
    public static List<ITuple> getJoinDistanceResults(IOperator outerOp, IOperator innerOp,
            String idAttrName, String joinAttrName, int threshold, int limit, int offset) throws TextDBException {
        JoinDistancePredicate distancePredicate = new JoinDistancePredicate(
                idAttrName, joinAttrName, threshold);
        Join join = new Join(distancePredicate);
        join.setInnerInputOperator(innerOp);
        join.setOuterInputOperator(outerOp);
        join.setLimit(limit);
        join.setOffset(offset);
        
        ITuple tuple;
        List<ITuple> results = new ArrayList<>();
        
        join.open();
        while ((tuple = join.getNextTuple()) != null) {
            results.add(tuple);
        }
        join.close();
        
        return results;
    }
    
    /**
     * Alter a field of a tuple. The schema will also be changed accordingly.
     * 
     * @param originalTuple
     * @param fieldIndex
     * @param newField
     * @return
     */
    public static ITuple alterField(ITuple originalTuple, int fieldIndex, IField newField) {
        List<Attribute> originalAttributes = originalTuple.getSchema().getAttributes();
        List<Attribute> newAttributes = new ArrayList<>();
        List<IField> newFields = new ArrayList<>();
        for (int i = 0; i < originalAttributes.size(); i++) {
            if (i == fieldIndex) {
                newAttributes.add(new Attribute(originalAttributes.get(i).getFieldName(), 
                        Utils.getFieldType(newField)));
                newFields.add(newField);
            } else {
                newAttributes.add(originalAttributes.get(i));
                newFields.add(originalTuple.getField(i));
            }
        }
        return new DataTuple(new Schema(newAttributes.stream().toArray(Attribute[]::new)), 
                newFields.stream().toArray(IField[]::new));
    }

}
