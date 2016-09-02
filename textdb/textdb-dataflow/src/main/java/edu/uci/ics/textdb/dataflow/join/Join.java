package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.JoinPredicate;


/**
 * The Join operator is an operator which intends to perform a "join" over the
 * the outputs of two other operators based on certain conditions defined
 * using the JoinPredicate.
 * 
 * The JoinPredicate currently takes:
 * ID attribute -> Which serves as the document/tuple ID. Only for the tuples
 * whose IDs match, we perform the join.
 * Join Attribute -> The attribute to perform Join on.
 * and Threshold -> The value within which the difference of span starts and
 * the difference of span ends should be for the join to take place.
 * 
 * Join takes two operators: innerOperator and outerOperator.
 * Each operator has a stream of output tuples, Join performs join on 
 * two tuples' span lists only if two tuples have the same ID.
 * 
 * Two operators usually have the same schema, but they don't necessarily have to.
 * Join requires two operators to share ID attribute and attribute to join.
 * For other attributes, join will perform an intersection on them.
 * 
 * Join assumes two tuples are the same if their ID are same.
 * If some attribute values of two tuples are different, if the attribute is the 
 * join attribute, the tuple is discarded. If the attribute is not join attribute,
 * then one of the values will be chosen to become the output value.
 * 
 * @author Sripad Kowshik Subramanyam (sripadks)
 *
 */
public class Join implements IOperator {

    private IOperator outerOperator;
    private IOperator innerOperator;
    private JoinPredicate joinPredicate;
    // To indicate if next result from outer operator has to be obtained.
    private boolean shouldIGetOuterOperatorNextTuple;
    private ITuple outerTuple = null;
    private ITuple innerTuple = null;
    private List<ITuple> innerTupleList = new ArrayList<>();
    // Cursor to maintain the position of tuple to be obtained from
    // innerTupleList.
    private Integer innerOperatorCursor = 0;
    private List<Attribute> outputAttrList;
    private Schema outputSchema;

    /**
     * This constructor is used to set the operators whose output is to be
     * compared and joined and the predicate which specifies the fields and
     * constraints over which join happens.
     * 
     * @param outer
     *            is the outer operator producing the tuples
     * @param inner
     *            is the inner operator producing the tuples
     * @param joinPredicate
     *            is the predicate over which the join is made
     */
    public Join(IOperator outerOperator, IOperator innerOperator, JoinPredicate joinPredicate) {
        this.outerOperator = outerOperator;
        this.innerOperator = innerOperator;
        this.joinPredicate = joinPredicate;
    }
    
    public Join(JoinPredicate joinPredicate) {
        this.joinPredicate = joinPredicate;
    }

    @Override
    public void open() throws Exception, DataFlowException {
        if (!(joinPredicate.getJoinAttribute().getFieldType().equals(FieldType.STRING)
                || joinPredicate.getJoinAttribute().getFieldType().equals(FieldType.TEXT))) {
            throw new Exception("Fields other than \"STRING\" and \"TEXT\" are not supported by Join yet.");
        }

        try {
            innerOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }

        shouldIGetOuterOperatorNextTuple = true;

        // Load the inner tuple list into memory on open.
        while ((innerTuple = innerOperator.getNextTuple()) != null) {
            innerTupleList.add(innerTuple);
        }

        // Close the inner operator as all the required tuples are already
        // loaded into memory.
        try {
            innerOperator.close();
            outerOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        
        generateIntersectionSchema();
        outputAttrList = outputSchema.getAttributes();
    }

    /**
     * Gets the next tuple which is a joint of two tuples which passed the
     * criteria set in the JoinPredicate. <br>
     * Example in JoinPredicate.java
     * 
     * @return nextTuple
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        if (innerTupleList.isEmpty()) {
            return null;
        }

        ITuple nextTuple = null;

        do {
            if (shouldIGetOuterOperatorNextTuple == true) {
                if ((outerTuple = outerOperator.getNextTuple()) == null) {
                    return null;
                }
                shouldIGetOuterOperatorNextTuple = false;
            }

            if (innerOperatorCursor <= innerTupleList.size() - 1) {
                innerTuple = innerTupleList.get(innerOperatorCursor);
                innerOperatorCursor++;
                if (innerOperatorCursor == innerTupleList.size()) {
                    innerOperatorCursor = 0;
                    shouldIGetOuterOperatorNextTuple = true;
                }
            }
            
            nextTuple = joinTuples(outerTuple, innerTuple, joinPredicate);
        } while (nextTuple == null);

        return nextTuple;
    }

    @Override
    public void close() throws Exception {
        try {
            outerOperator.close();
            // innerOperator.close(); already called in open()

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        // Clear the inner tuple list from memory on close.
        innerTupleList.clear();
    }

    // compare if two tuples have the save value over a field
    // return null if one of them doesn't have this field
    private boolean compareField(ITuple innerTuple, ITuple outerTuple, String fieldName) {  
        IField innerField = innerTuple.getField(fieldName);
        IField outerField = outerTuple.getField(fieldName);
        
        if (innerField == null ||  outerField == null) {
            return false;
        }

        return innerField.getValue().equals(outerField.getValue());
    }

    // Process the tuples to get a tuple with join result if predicate is
    // satisfied.
    private ITuple joinTuples(ITuple outerTuple, ITuple innerTuple, JoinPredicate joinPredicate) throws Exception {
        List<Span> newJoinSpanList = new ArrayList<>();
        
        // We expect the values of all fields to be the same for innerTuple and outerTuple.
        // We only checks ID field, and field to be joined, since they are crucial to join operator.
        // For other fields, we use the value from innerTuple.

        // check if the ID fields are the same
        if (! compareField(innerTuple, outerTuple, joinPredicate.getIDAttribute().getFieldName())) {
            return null;
        }

        // check if the fields to be joined are the same
        if (! compareField(innerTuple, outerTuple, joinPredicate.getJoinAttribute().getFieldName())) {
            return null;
        }
        
        
        // If either/both tuples have no span information, return null.
        // Check using try/catch if both the tuples have span information.
        // If not return null; so we can process next tuple.

        IField spanFieldOfInnerTuple = null;
        IField spanFieldOfOuterTuple = null;
        try {
            spanFieldOfInnerTuple = innerTuple.getField(SchemaConstants.SPAN_LIST);
            spanFieldOfOuterTuple = outerTuple.getField(SchemaConstants.SPAN_LIST);
        } catch (Exception e) {
            return null;
        }

        List<Span> innerSpanList = null;
        List<Span> outerSpanList = null;
        // Check if both the fields obtained from the indexes are indeed of type
        // ListField
        if (spanFieldOfInnerTuple.getClass().equals(ListField.class)) {
            innerSpanList = (List<Span>) spanFieldOfInnerTuple.getValue();
        }
        if (spanFieldOfOuterTuple.getClass().equals(ListField.class)) {
            outerSpanList = (List<Span>) spanFieldOfOuterTuple.getValue();
        }

        Iterator<Span> outerSpanIter = outerSpanList.iterator();

        // TODO Currently we are using two loops to go over two span lists.
        // We can optimize it by sorting the spans based on the start position
        // and then doing a "sort merge" of the two lists.
        // (Also probably weed out the spans with fields that don't agree with
        // the ones specified in the JoinPredicate during "sort merge"?)
        while (outerSpanIter.hasNext()) {
            Span outerSpan = outerSpanIter.next();
            // Check if the field matches the filed over which we want to join.
            // If not return null.
            if (!outerSpan.getFieldName().equals(joinPredicate.getJoinAttribute().getFieldName())) {
                continue;
            }
            Iterator<Span> innerSpanIter = innerSpanList.iterator();
            while (innerSpanIter.hasNext()) {
                Span innerSpan = innerSpanIter.next();
                if (!innerSpan.getFieldName().equals(joinPredicate.getJoinAttribute().getFieldName())) {
                    continue;
                }
                Integer threshold = joinPredicate.getThreshold();
                if (Math.abs(outerSpan.getStart() - innerSpan.getStart()) <= threshold
                        && Math.abs(outerSpan.getEnd() - innerSpan.getEnd()) <= threshold) {
                    Integer newSpanStartIndex = Math.min(outerSpan.getStart(), innerSpan.getStart());
                    Integer newSpanEndIndex = Math.max(outerSpan.getEnd(), innerSpan.getEnd());
                    String fieldName = joinPredicate.getJoinAttribute().getFieldName();
                    String fieldValue = (String) innerTuple.getField(fieldName).getValue();
                    String newFieldValue = fieldValue.substring(newSpanStartIndex, newSpanEndIndex);
                    String spanKey = outerSpan.getKey() + "_" + innerSpan.getKey();
                    Span newSpan = new Span(fieldName, newSpanStartIndex, newSpanEndIndex, spanKey, newFieldValue);
                    newJoinSpanList.add(newSpan);
                }
            }
        }

        if (newJoinSpanList.isEmpty()) {
            return null;
        }
       
        // create output fields based on innerTuple's value
        List<IField> outputFields = 
                outputAttrList.stream()
                .filter(attr -> ! attr.equals(SchemaConstants.SPAN_LIST_ATTRIBUTE))
                .map(attr -> attr.getFieldName())
                .map(fieldName -> innerTuple.getField(fieldName))
                .collect(Collectors.toList());
        
        outputFields.add(new ListField<>(newJoinSpanList));
        
        return new DataTuple(outputSchema, outputFields.stream().toArray(IField[]::new));
    }
    
    
    /*
     * Create outputSchema, which is the intersection of innerOperator's schema and outerOperator's schema.
     * The attributes have to be exactly the same (name and type) to be intersected.
     * 
     * InnerOperator's attributes and outerOperator's attributes must:
     * both contain the attributes to be joined.
     * both contain "ID" attribute. (ID attribute that user specifies in joinPredicate)
     * both contain "spanList" attribute.
     * 
     */
    private void generateIntersectionSchema() throws DataFlowException {
        List<Attribute> innerAttributes = innerOperator.getOutputSchema().getAttributes();
        List<Attribute> outerAttributes = outerOperator.getOutputSchema().getAttributes();
        
        List<Attribute> intersectionAttributes = 
                innerAttributes.stream()
                .filter(attr -> outerAttributes.contains(attr))
                .collect(Collectors.toList());
        
        if (intersectionAttributes.isEmpty()) {
            throw new DataFlowException("inner operator and outer operator don't share any common attributes");
        } else if (! intersectionAttributes.contains(joinPredicate.getJoinAttribute())) {
            throw new DataFlowException("inner operator or outer operator doesn't contain join attribute");
        } else if (! intersectionAttributes.contains(joinPredicate.getIDAttribute())) {
            throw new DataFlowException("inner operator or outer operator doesn't contain ID attribute");
        } else if (! intersectionAttributes.contains(SchemaConstants.SPAN_LIST_ATTRIBUTE)) {
            throw new DataFlowException("inner operator or outer operator doesn't contain spanList attribute");
        } 
        
        outputSchema = new Schema(intersectionAttributes.stream().toArray(Attribute[]::new));
    }
    
    public void setInnerInputOperator(IOperator innerInputOperator) {
        this.innerOperator = innerInputOperator;
    }
    
    public void setOuterInputOperator(IOperator outerInputOperator) {
        this.outerOperator = outerInputOperator;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    public IOperator getOuterOperator() {
        return outerOperator;
    }

    public void setOuterOperator(IOperator outerOperator) {
        this.outerOperator = outerOperator;
    }

    public IOperator getInnerOperator() {
        return innerOperator;
    }

    public void setInnerOperator(IOperator innerOperator) {
        this.innerOperator = innerOperator;
    }
}
