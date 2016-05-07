package edu.uci.ics.textdb.storage;

import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;

/**
 * Created by sandeepreddy602 on 05-06-2016.
 */
public class DataReaderPredicate implements IPredicate {
    private IDataStore dataStore;
    private Query query;

    public DataReaderPredicate(IDataStore dataStore, Query query){
        this.dataStore = dataStore;
        this.query = query;
    }

    public IDataStore getDataStore() {
        return dataStore;
    }

    public Query getQuery() {
        return query;
    }
}
