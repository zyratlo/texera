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
import edu.uci.ics.textdb.common.field.StringField;

/**
 * Created by chenli on 3/25/16.
 */
public class RegexMatcher implements IOperator {
    private final IPredicate predicate;
    private ISourceOperator sourceOperator;
    private Query luceneQuery;
    
    private int positionIndex; // next position in the field to be checked.
    private int fieldIndex; // Index of the next field to be checked.
    private int spanIndexValue; // Starting position of the matched dictionary
                                // string
    
    private String fieldName;
    private ITuple dataTuple;
    private List<IField> fields;
    private List<Attribute> schema;

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
            if (predicate.satisfy(sourceTuple)) {
                return sourceTuple;
            } else {
                return getNextTuple();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }

    }
    
    private ITuple getSpanTuple() {
    	List<Attribute> schemaDuplicate = new ArrayList<>(schema);
    	schemaDuplicate.add(SchemaConstants.SPAN_FIELD_NAME_ATTRIBUTE);
    	schemaDuplicate.add(SchemaConstants.SPAN_KEY_ATTRIBUTE);
    	schemaDuplicate.add(SchemaConstants.SPAN_BEGIN_ATTRIBUTE);
    	schemaDuplicate.add(SchemaConstants.SPAN_END_ATTRIBUTE);
    	
    	List<IField> fieldListDuplicate = new ArrayList<>(fields);
//    	fieldListDuplicate.add(new StringField(fieldName));
//    	fieldListDuplicate.add(new StringField(fieldName));
//    	fieldListDuplicate.add(new StringField(fieldName));
    	
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
