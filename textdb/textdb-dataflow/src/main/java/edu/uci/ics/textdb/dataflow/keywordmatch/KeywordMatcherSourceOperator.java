package edu.uci.ics.textdb.dataflow.keywordmatch;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;

/**
 * KeywordMatcherSourceOperator is a source operator with a keyword query.
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordMatcherSourceOperator extends AbstractSingleInputOperator implements ISourceOperator  {
    
    private KeywordPredicate predicate;
    private IDataStore dataStore;
    
    private IndexBasedSourceOperator indexSource;
    private KeywordMatcher keywordMatcher;
    
    private Schema outputSchema;

    protected int cursor = CLOSED;

    
    public KeywordMatcherSourceOperator(KeywordPredicate predicate, IDataStore dataStore) {
        this.predicate = predicate;
        this.dataStore = dataStore;
        
        indexSource = new IndexBasedSourceOperator(predicate.generateDataReaderPredicate(dataStore));
        keywordMatcher = new KeywordMatcher(predicate);
        keywordMatcher.setInputOperator(indexSource);
        inputOperator = this.keywordMatcher;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

    @Override
    protected void setUp() throws DataFlowException {
        this.outputSchema = this.keywordMatcher.getOutputSchema();        
    }

    @Override
    protected ITuple computeNextMatchingTuple() throws Exception {
        return this.keywordMatcher.getNextTuple();
    }

    @Override
    protected void cleanUp() throws DataFlowException {        
    }
    
    /**
     * Source Operator doesn't need input operator
     */
    @Override
    public void setInputOperator(IOperator inputOperator) {
    }
    
    public KeywordPredicate getPredicate() {
        return this.predicate;
    }
    
    public IDataStore getDataStore() {
        return this.dataStore;
    }

}
