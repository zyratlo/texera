package edu.uci.ics.textdb.storage;

import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;

/**
 * Created by sandeepreddy602 on 05-06-2016.
 */
public class DataReaderPredicate implements IPredicate {
    private IDataStore dataStore;
    private Query luceneQuery;

    public DataReaderPredicate(IDataStore dataStore, Query luceneQuery){
        this.dataStore = dataStore;
        this.luceneQuery = luceneQuery;
    }

    public IDataStore getDataStore() {
        return dataStore;
    }

    public Query getLuceneQuery() {
        return luceneQuery;
    }
}
