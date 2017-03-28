package edu.uci.ics.textdb.exp.sampler;

import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * 
 * @author Qinhua Huang
 *
 */

public class SamplerTest {
    
    public static final String SAMPLER_TABLE = "sampler_test";
    private static int indexSize;
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        // create the people table and write tuples
        
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
        RelationManager relationManager2 = RelationManager.getRelationManager();
        relationManager2.createTable(SAMPLER_TABLE, "../index/test_tables/" + SAMPLER_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter regexDataWriter = relationManager2.getTableDataWriter(SAMPLER_TABLE);
        regexDataWriter.open();
        indexSize = 0;
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            regexDataWriter.insertTuple(tuple);
            indexSize ++;
        }
        regexDataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
    }
    
    public static List<Tuple> getSampleResults( String tableName, int k ) throws TextDBException{
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(tableName);
        Sampler tupleSampler = new Sampler(new SamplerPredicate(k));
        tupleSampler.setInputOperator(scanSource);
        
        List<Tuple> results = new ArrayList<>();
        Tuple tuple;
        
        tupleSampler.open();
        while((tuple = tupleSampler.getNextTuple()) != null) {
            results.add(tuple);
        }
        tupleSampler.close();
        return results;
    }
    
    /*
     * Sampled a middle number between [0, indexSize]
     */
    @Test
    public void test1() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,2);
        Assert.assertEquals(results.size(), 2);
    }
    
    /*
     * Sampled 0 tuples
     */
    @Test
    public void test2() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,0);
        Assert.assertEquals(results.size(), 0);
    }
    
    /*
     * Sampled all tuples
     */
    @Test
    public void test3() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,indexSize);
        Assert.assertEquals(results.size(), indexSize);
    }
    
    /*
     * Sampled more than max tuples
     * It will return all the tuples in the index.
     */
    @Test
    public void test4() throws TextDBException {
        List<Tuple> results = getSampleResults(SAMPLER_TABLE,indexSize+1);
        Assert.assertEquals(results.size(), indexSize);
    }
}
