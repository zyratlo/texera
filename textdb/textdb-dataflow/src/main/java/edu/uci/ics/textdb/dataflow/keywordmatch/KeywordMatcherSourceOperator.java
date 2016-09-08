package edu.uci.ics.textdb.dataflow.keywordmatch;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;

public class KeywordMatcherSourceOperator implements ISourceOperator {
    
    private KeywordPredicate predicate;
    private IDataStore dataStore;
    
    private ISourceOperator indexSource;
    private KeywordMatcher keywordMatcher;
    
    private Schema outputSchema;

    protected int cursor = CLOSED;

    
    public KeywordMatcherSourceOperator(KeywordPredicate predicate, IDataStore dataStore) {
        this.predicate = predicate;
        this.dataStore = dataStore;
    }

    @Override
    public void open() throws Exception {
        if (cursor != CLOSED) {
            return;
        }
        try {
            indexSource = new IndexBasedSourceOperator(predicate.generateDataReaderPredicate(dataStore));
            keywordMatcher = new KeywordMatcher(predicate);
            keywordMatcher.setInputOperator(indexSource);
            
            keywordMatcher.open();
            outputSchema = keywordMatcher.getOutputSchema();
            
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = OPENED;        
    }

    @Override
    public ITuple getNextTuple() throws Exception {
        return keywordMatcher.getNextTuple();
    }

    @Override
    public void close() throws Exception {
        if (cursor == CLOSED) {
            return;
        }
        try {
            if (keywordMatcher != null) {
                keywordMatcher.close();
            }
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }
    
    public KeywordPredicate getPredicate() {
        return this.predicate;
    }
    
    public IDataStore getDataStore() {
        return this.dataStore;
    }

}
