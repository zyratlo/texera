package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;

/**
 * Created by chenli on 3/25/16.
 * @author laishuying
 */
public class RegexMatcher implements IOperator {
    private final IPredicate predicate;
    private ISourceOperator sourceOperator;
    private Query luceneQuery;
    
    private String spanFieldName;
    private String spanKey;

    private List<IField> fields;
    private List<Attribute> schema;
    
    private List<Span> spans;
    private int nextSpanIndex = -1; //index of next span object to be returned 

    public RegexMatcher(IPredicate predicate, ISourceOperator sourceOperator) {
        this.predicate = predicate;
        this.sourceOperator = sourceOperator;
        //TODO build the luceneQuery by given regex.
    }

    @Override
    public void open() throws DataFlowException {
        try {
            sourceOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
    	if (nextSpanIndex == -1 || nextSpanIndex == spans.size()) {
    		try {
                ITuple sourceTuple = sourceOperator.getNextTuple();
                if(sourceTuple == null){
                    return null;
                }
                
                RegexPredicate rPredicate = (RegexPredicate)predicate; 
                fields = sourceTuple.getFields();
                schema = sourceTuple.getSchema();
                spanFieldName = rPredicate.getFieldName();
                spanKey = (String)sourceTuple.getField(spanFieldName).getValue();
                
                spans = rPredicate.statisfySpan(sourceTuple);
                
                if (spans.size() != 0) { // at least one match found
                	nextSpanIndex = 1;
                	return getSpanTuple(spans.get(0));
                } else { // no match found
                	nextSpanIndex = -1;
                	return getNextTuple();
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new DataFlowException(e.getMessage(), e);
            }
    	} else { //if current field (current list of span) has not been consumed
    		nextSpanIndex ++;
    		return getSpanTuple(spans.get(nextSpanIndex-1)); //return the next span in the list
    	}
        

    }
    
    private ITuple getSpanTuple(Span span) {
    	List<Attribute> schemaDuplicate = new ArrayList<>(schema);
    	schemaDuplicate.add(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE);
    	schemaDuplicate.add(SchemaConstants.SPAN_KEY_ATTRIBUTE);
    	schemaDuplicate.add(SchemaConstants.SPAN_BEGIN_ATTRIBUTE);
    	schemaDuplicate.add(SchemaConstants.SPAN_END_ATTRIBUTE);
    	
    	List<IField> fieldListDuplicate = new ArrayList<>(fields);
    	fieldListDuplicate.add(new StringField(spanFieldName));
    	fieldListDuplicate.add(new StringField(spanKey));
    	fieldListDuplicate.add(new IntegerField(span.getStart()));
    	fieldListDuplicate.add(new IntegerField(span.getEnd()));
    	
    	IField[]  fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
    	return new DataTuple(schemaDuplicate, fieldsDuplicate);
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
