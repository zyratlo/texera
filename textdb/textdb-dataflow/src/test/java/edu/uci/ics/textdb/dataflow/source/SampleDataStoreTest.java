/**
 * 
 */
package edu.uci.ics.textdb.dataflow.source;

import org.junit.Before;
import org.junit.Test;

/**
 * @author sandeepreddy602
 *
 */
public class SampleDataStoreTest {
    private SampleDataStore dataStore;
    
    @Before
    public void setUp(){
        dataStore = new SampleDataStore();
    }
    
    @Test
    public void testStoreData() throws Exception{
        dataStore.clearData();
        dataStore.storeData();
        System.out.println("Stored the sample data into Lucene DB");
    }
}
