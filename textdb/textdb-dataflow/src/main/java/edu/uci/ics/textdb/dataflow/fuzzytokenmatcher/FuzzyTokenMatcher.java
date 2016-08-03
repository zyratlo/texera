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
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.FuzzyTokenPredicate;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

public class FuzzyTokenMatcher implements IOperator{
    private final FuzzyTokenPredicate predicate;
    private ISourceOperator sourceOperator;
    private List<Attribute> attributeList;
    private int threshold;
    private ArrayList<String> queryTokens;
    private int limit;
    private int cursor;
    private int offset;

    public FuzzyTokenMatcher(IPredicate predicate) {
    	this.predicate = (FuzzyTokenPredicate)predicate;
    	DataReaderPredicate dataReaderPredicate = this.predicate.getDataReaderPredicate();
    	this.sourceOperator = new IndexBasedSourceOperator(dataReaderPredicate);
    }
    
    @Override
    public void open() throws DataFlowException {
    	try {
    		cursor = 0;
    		limit = Integer.MAX_VALUE;
    		offset = 0;
            sourceOperator.open();
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
			//System.out.println(cursor);
			if (cursor >= limit + offset){
				return null;
			}
			ITuple result = null;
			while (true) {
			    ITuple sourceTuple = sourceOperator.getNextTuple();
			    if (sourceTuple == null) {
			    	return null;
			    }
		    	result = processTuple(sourceTuple);
		    	if (result != null) {
		    		cursor++;
		    	}
		    	if (cursor > offset) {
		    		break;
		    	}
			}
		    return result;
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
    
    public void setOffset(int offset, int rowCount){
    	this.offset = offset;
    	this.limit = rowCount;
    }
    
    public int getOffset(){
    	return this.offset;
    }

    private ITuple processTuple(ITuple currentTuple){
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
			sourceOperator.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
    }
}
