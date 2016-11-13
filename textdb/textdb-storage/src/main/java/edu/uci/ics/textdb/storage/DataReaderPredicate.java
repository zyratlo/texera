package edu.uci.ics.textdb.storage;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataStore;

/**
 * Created by sandeepreddy602 on 05-06-2016.
 */
public class DataReaderPredicate implements IPredicate {
    private IDataStore dataStore;
    private Query luceneQuery;
    private String queryString;
    private Analyzer luceneAnalyzer;
    private List<String> attributeNames;
    private boolean isSpanInformationAdded = false;

    public DataReaderPredicate(Query luceneQuery, String queryString, IDataStore dataStore,
            List<String> attributeNames, Analyzer analyzer) {
        this.dataStore = dataStore;
        this.luceneQuery = luceneQuery;
        this.luceneAnalyzer = analyzer;
        this.queryString = queryString;
        this.attributeNames = attributeNames;
    }

    public void setIsSpanInformationAdded(boolean flag) {
        isSpanInformationAdded = flag;
    }

    public IDataStore getDataStore() {
        return dataStore;
    }

    public Query getLuceneQuery() {
        return luceneQuery;
    }

    public String getQueryString() {
        return queryString;
    }

    public Analyzer getLuceneAnalyzer() {
        return luceneAnalyzer;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    public boolean getIsSpanInformationAdded() {
        return isSpanInformationAdded;
    }
}
