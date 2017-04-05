package edu.uci.ics.textdb.exp.sampler;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.exp.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.ScanSourcePredicate;
import edu.uci.ics.textdb.exp.sampler.SamplerPredicate.SampleType;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Qinhua Huang
 */

public class SamplerTest {
    
    public static final String SAMPLER_TABLE = "sampler_test";
    private static int indexSize;
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        // Create the people table and write tuples
        
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
        relationManager.createTable(SAMPLER_TABLE, "../index/test_tables/" + SAMPLER_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter dataWriter = relationManager.getTableDataWriter(SAMPLER_TABLE);
        dataWriter.open();
        indexSize = 0;
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            dataWriter.insertTuple(tuple);
            indexSize ++;
        }
        dataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
    }
    
    public static List<Tuple> computeSampleResults(String tableName, int k,
            SampleType sampleType) throws TextDBException {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        Sampler tupleSampler = new Sampler(new SamplerPredicate(k, sampleType));
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
     * To test if all the sampled tuples are in the sampler table.
     */
    public static boolean containedInSamplerTable(List<Tuple> sampleList) throws TextDBException {
        ScanBasedSourceOperator scanSource = 
                new ScanBasedSourceOperator(new ScanSourcePredicate(SAMPLER_TABLE));
        
        scanSource.open();
        Tuple nextTuple = null;
        List<Tuple> returnedTuples = new ArrayList<Tuple>();
        while ((nextTuple = scanSource.getNextTuple()) != null) {
            returnedTuples.add(nextTuple);
        }
        scanSource.close();
        boolean contains = TestUtils.containsAll(returnedTuples, sampleList);
        return contains;
    }
    
    /* 
     * To test if the sampled tuples equal to the first K tuples of the sampler table 
     * in both order and content.
     */
    public static boolean isSameOrderWithTable(List<Tuple> sampleList) throws TextDBException {
        ScanBasedSourceOperator scanSource = 
                new ScanBasedSourceOperator(new ScanSourcePredicate(SAMPLER_TABLE));
        
        scanSource.open();
        ListIterator<Tuple> itor = null;
        itor = sampleList.listIterator();
        while (itor.hasNext()) {
            Tuple nextTableTuple = scanSource.getNextTuple();
            Tuple nextSampledTuple = itor.next();
            
            if (!nextSampledTuple.equals(nextTableTuple)) {
                scanSource.close();
                return false;
            }
        }
        scanSource.close();
        return true;
    }
    
    /*
     * Sample 0 tuple in FIRST_K_ARRIVAL mode
     */
    @Test(expected = RuntimeException.class)
    public void test1() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,0, SampleType.FIRST_K_ARRIVAL);
    }
    
    /*
     * Sample the first tuple in FIRST_K_ARRIVAL mode.
     */
    @Test
    public void test2() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,1, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), 1);
        Assert.assertTrue(containedInSamplerTable(results));
        Assert.assertTrue(isSameOrderWithTable(results));
    }
    
    /*
     * Sample all the tuples in FIRST_K_ARRIVAL mode.
     */
    @Test
    public void test3() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(containedInSamplerTable(results));
        Assert.assertTrue(isSameOrderWithTable(results));
    }
    
    /*
     * Try to sample tuples more than the previous operator can get in FIRST_K_ARRIVAL mode.
     * It should return all tuples get from the previous operator as result.
     */
    @Test
    public void test4() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize+1, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(containedInSamplerTable(results));
        Assert.assertTrue(isSameOrderWithTable(results));
    }
    
    /*
     * Sample tuples in number between 0 and indexSize in RANDOM_SAMPLE mode
     */
    @Test
    public void test5() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,2, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), 2);
        Assert.assertTrue(containedInSamplerTable(results));
    }
    
    /*
     * Sample zero tuple in RANDOM_SAMPLE mode.
     */
    @Test(expected = RuntimeException.class)
    public void test6() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,0, SampleType.RANDOM_SAMPLE);
    }
    
    /*
     * Sample all tuples in RANDOM_SAMPLE mode.
     * It should output all tuples get from previous operator in same order.
     */
    @Test
    public void test7() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(containedInSamplerTable(results));
        Assert.assertTrue(isSameOrderWithTable(results));
    }
    
    /*
     * Try to sample tuples more than the previous operator can get in RANDOM_SAMPLE mode.
     * It should return all tuples get from the previous operator as result in same order..
     */
    @Test
    public void test8() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize+1, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(containedInSamplerTable(results));
        Assert.assertTrue(isSameOrderWithTable(results));
    }
}
