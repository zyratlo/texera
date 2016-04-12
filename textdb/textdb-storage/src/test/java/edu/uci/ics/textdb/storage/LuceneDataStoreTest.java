package edu.uci.ics.textdb.storage;

import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;

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
                LuceneConstants.INDEX_DIR, TestConstants.SAMPLE_SCHEMA_PEOPLE);
        List<ITuple> tuplesFetched = luceneDataReader.getTuples();

        Assert.assertEquals(tuples.size(), tuplesFetched.size());
        
    }
}
