package edu.uci.ics.textdb.dataflow.fuzzytokenmatcher;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
/**
 *  @author Parag Saraogi, Varun Bharill
 *  This class takes a predicate as input, constructs the source operator, executes the query
 *  and returns the results iteratively
 */
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

public class FuzzyTokenMatcher implements IOperator{
    private final FuzzyTokenPredicate predicate;
    private IOperator inputOperator;

	private List<Attribute> attributeList;
    private int threshold;
    private ArrayList<String> queryTokens;
    private int limit;
    private int cursor;
    private int offset;

    public FuzzyTokenMatcher(IPredicate predicate) {
        this.cursor = -1;
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.predicate = (FuzzyTokenPredicate)predicate;
        DataReaderPredicate dataReaderPredicate = this.predicate.getDataReaderPredicate();
        this.inputOperator = new IndexBasedSourceOperator(dataReaderPredicate);
    }
    
    @Override
    public void open() throws DataFlowException {
    	if (this.inputOperator == null) {
    		throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
    	}
    	try {
            inputOperator.open();
            attributeList = predicate.getAttributeList();
            threshold = predicate.getThreshold();
            queryTokens = predicate.getQueryTokens();
    	} catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
    	}
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
		try {
			if (limit == 0 || cursor >= limit + offset - 1){
				return null;
			}
			ITuple sourceTuple;
			ITuple resultTuple = null;
			while ((sourceTuple = inputOperator.getNextTuple()) != null) {
		    	resultTuple = computeNextTuple(sourceTuple);
		    	if (resultTuple != null) {
		    		cursor++;
		    	}
		    	if (cursor >= offset) {
		    		break;
		    	}
			}
		    return resultTuple;
		} catch (Exception e) {
		    e.printStackTrace();
		    throw new DataFlowException(e.getMessage(), e);
		}
    }
    
    public void setLimit(int limit){
    	this.limit = limit;
    }
    
    public int getLimit(){
    	return this.limit;
    }
    
    public void setOffset(int offset){
    	this.offset = offset;
    }
    
    public int getOffset(){
    	return this.offset;
    }

    private ITuple computeNextTuple(ITuple currentTuple){
    	if (currentTuple == null) {
    		return null;
    	}
    	if (! this.predicate.getIsSpanInformationAdded()) {
    		return currentTuple;
    	}
	    int schemaIndex = currentTuple.getSchema().getIndex(SchemaConstants.SPAN_LIST_ATTRIBUTE.getFieldName());
	    List<Span> resultSpans =
                (List<Span>)currentTuple.getField(schemaIndex).getValue();
        
	    /*The source operator returns spans even for those fields which did not satisfy the threshold criterion.
	     *  So if two attributes A,B have 10 and 5 matching tokens, and we set threshold to 10,
	     *  the number of spans returned is 15. So we need to filter those 5 spans for attribute B.
	    */
	    for(int attributeIndex = 0; attributeIndex < attributeList.size(); attributeIndex++) {
	        String fieldName = attributeList.get(attributeIndex).getFieldName();
	        IField field = currentTuple.getField(fieldName);
            
	        if (field instanceof TextField) {         //Lucene defines Fuzzy Token Matching only for text fields.
	            int tokensMatched = 0;
	            List<Span> attributeSpans = new ArrayList<>();
	            for (Span span : resultSpans) {
	                if (span.getFieldName().equals(fieldName)) {
	                    attributeSpans.add(span);
	                    if (queryTokens.contains(span.getKey()))
	                        tokensMatched++;
	                }
	            }
	            if (tokensMatched < threshold) {
	                resultSpans.removeAll(attributeSpans);
	            }
	        }
	    }
	    return currentTuple;
    }
    
    @Override
    public void close() throws DataFlowException {
		try {
        	if (inputOperator != null) {
                inputOperator.close();
        	}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
    }
    
    public IOperator getInputOperator() {
		return inputOperator;
	}

	public void setInputOperator(ISourceOperator inputOperator) {
		this.inputOperator = inputOperator;
	}
}
