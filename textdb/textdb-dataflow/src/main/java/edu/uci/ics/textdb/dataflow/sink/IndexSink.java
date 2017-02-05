package edu.uci.ics.textdb.dataflow.sink;

import edu.uci.ics.textdb.api.exception.TextDBException;
import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.DataWriter;

/**
 * IndexSink is a sink that writes tuples into an index.
 * 
 * @author zuozhi
 */
public class IndexSink extends AbstractSink {

    private DataWriter dataWriter;
    private boolean isAppend = false;

    public IndexSink(String indexDirectory, Schema schema, Analyzer luceneAnalyzer, boolean isAppend) {
        DataStore dataStore = new DataStore(indexDirectory, schema);
        this.dataWriter = new DataWriter(dataStore, luceneAnalyzer);
        this.isAppend = isAppend;
    }

    public void open() throws TextDBException {
        super.open();
        if (! this.isAppend) {
            this.dataWriter.clearData();
        }
        this.dataWriter.open();
    }

    protected void processOneTuple(ITuple nextTuple) throws TextDBException {
        dataWriter.insertTuple(nextTuple);
    }

    public void close() throws TextDBException {
        if (this.dataWriter != null) {
            this.dataWriter.close();
        }
        super.close();
    }

}
