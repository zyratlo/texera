package edu.uci.ics.textdb.dataflow.fuzzytokenmatcher;

import java.util.ArrayList;
import java.util.Iterator;
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
import edu.uci.ics.textdb.api.common.Schema;
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
    
    private Schema outputSchema;

	private List<Attribute> attributeList;
    private int threshold;
    private ArrayList<String> queryTokens;

    public FuzzyTokenMatcher(IPredicate predicate) {
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
            outputSchema = inputOperator.getOutputSchema();
    	} catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
    	}
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
		try {
		    ITuple result = null;
		    
		    while (result == null) {
		        ITuple sourceTuple = inputOperator.getNextTuple();
		        if (sourceTuple == null) {
		            return null;
		        }
		        if (! this.predicate.getIsSpanInformationAdded()) {
		            return sourceTuple;
		        }
		        
		        result = processTuple(sourceTuple);
		        
		    }

		    return result;
		} catch (Exception e) {
		    e.printStackTrace();
		    throw new DataFlowException(e.getMessage(), e);
		}
    }
    
    private ITuple processTuple(ITuple currentTuple) {
        List<Span> payload = (List<Span>) currentTuple.getField(SchemaConstants.SPAN_LIST).getValue(); 
        List<Span> relevantSpans = filterRelevantSpans(payload);
        List<Span> matchResults = new ArrayList<>();
        
        /*The source operator returns spans even for those fields which did not satisfy the threshold criterion.
         *  So if two attributes A,B have 10 and 5 matching tokens, and we set threshold to 10,
         *  the number of spans returned is 15. So we need to filter those 5 spans for attribute B.
        */
        for(int attributeIndex = 0; attributeIndex < attributeList.size(); attributeIndex++) {
            String fieldName = attributeList.get(attributeIndex).getFieldName();
            IField field = currentTuple.getField(fieldName);
            
            List<Span> fieldSpans = new ArrayList<>();
            
            if (field instanceof TextField) {         //Lucene defines Fuzzy Token Matching only for text fields.
                for (Span span : relevantSpans) {
                    if (span.getFieldName().equals(fieldName)) {
                        if (queryTokens.contains(span.getKey())) {
                            fieldSpans.add(span);  
                        }
                    }
                }
            }
            
            if (fieldSpans.size() >= threshold) {
                matchResults.addAll(fieldSpans);
            }

        }
        
        if (matchResults.isEmpty()) {
            return null;
        }
        
        // temporarily delete all spans in payload to pass all test cases
        payload.clear();  // TODO: delete this line after DataReader's changes
        
        List<Span> spanList = (List<Span>) currentTuple.getField(SchemaConstants.SPAN_LIST).getValue();
        spanList.addAll(matchResults);
        
        return currentTuple;
    }
    
    private List<Span> filterRelevantSpans(List<Span> spanList) {
        List<Span> relevantSpans = new ArrayList<>();
        Iterator<Span> iterator = spanList.iterator();
        while (iterator.hasNext()) {
            Span span  = iterator.next();
            if (predicate.getQueryTokens().contains(span.getKey())) {
                relevantSpans.add(span);
            }
        }
        return relevantSpans;
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

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
}
