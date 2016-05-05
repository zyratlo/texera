package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;

/**
 * Created by chenli on 3/25/16.
 * @author laishuying
 */
public class RegexMatcher implements IOperator {
    private final IPredicate predicate;
    private ISourceOperator sourceOperator;
    private Query luceneQuery;

    private List<IField> fields;
    private Schema schema;
    private Schema spanSchema;
    
    private List<Span> spans;

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
		try {
            ITuple sourceTuple = sourceOperator.getNextTuple();
            if(sourceTuple == null){
                return null;
            }
            
            RegexPredicate regexPredicate = (RegexPredicate)predicate; 
            
            spans = regexPredicate.computeMatches(sourceTuple);
            
            if (spans != null && spans.size() != 0) { // a list of matches found
            	if (schema == null || spanSchema == null) {
            		schema = sourceTuple.getSchema();
            		spanSchema = createSpanSchema();
            	}
            	fields = sourceTuple.getFields();
            	return getSpanTuple(spans);
            } else { // no match found
            	return getNextTuple();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }        
    }
    
    /**
     * This function converts the schema of original tuples to the schema of match results returned by RegexMatcher. 
     * It adds SPAN_LIST_ATTRIBUTE in the end of the schema of tuples from lower level.
     * Because we need to use span to specify the position of matched sequence in this tuple.
     * For example, if a tuple ("george watson", "staff", 33, "(949)888-8888") matched "g[^\s]*",
     * RegexMatcher returns ("george watson", "staff", 33, "(949)888-8888", [Span(name, 0, 6, "g[^\s]*", "george watson")])
     * 
     * @return a schema object = attributes in original tuples followed by SPAN_LIST_ATTRIBUTE
     */
    private Schema createSpanSchema() {
    	List<Attribute> attributesCopy = new ArrayList<>(schema.getAttributes());
    	attributesCopy.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);
    	return new Schema(attributesCopy.toArray(new Attribute[attributesCopy.size()]));
    }
    
    private ITuple getSpanTuple(List<Span> spans) {
    	List<IField> fieldListDuplicate = new ArrayList<>(fields);
    	IField spanListField = new ListField<Span>(spans);
    	fieldListDuplicate.add(spanListField);
    	
    	IField[]  fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
    	return new DataTuple(spanSchema, fieldsDuplicate);
    }
    
    public Schema getSpanSchema() {
    	return spanSchema;
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
