package edu.uci.ics.textdb.storage;

import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.dataflow.LuceneDataReader;

public class LuceneDataStoreTest {
    private LuceneDataStore dataStore;
    
    @Before
    public void setUp(){
        dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR);
    }
    
    @Test
    public void testStoreData() throws Exception{
        dataStore.clearData();
        List<Attribute> schema = TestConstants.SAMPLE_SCHEMA_PEOPLE;
        List<ITuple> tuples = TestConstants.getSamplePeopleTuples();
        dataStore.storeData(schema, tuples);
        
        LuceneDataReader luceneDataReader = new LuceneDataReader(
                LuceneConstants.INDEX_DIR, TestConstants.SAMPLE_SCHEMA_PEOPLE, 
                LuceneConstants.SCAN_QUERY, TestConstants.SAMPLE_SCHEMA_PEOPLE.get(0).getFieldName());
        luceneDataReader.open();
        ITuple nextTuple = null;
        int numTuples = 0;
        while((nextTuple  = luceneDataReader.getNextTuple()) != null){
            //Checking if the tuple retrieved is present in the samplesTuples
            boolean contains = contains(tuples, nextTuple);
            Assert.assertTrue(contains);
            numTuples ++;
        }
        Assert.assertEquals(tuples.size(), numTuples);
        luceneDataReader.close();
    }

    private boolean contains(List<ITuple> sampleTuples, ITuple actualTuple) {
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
}
