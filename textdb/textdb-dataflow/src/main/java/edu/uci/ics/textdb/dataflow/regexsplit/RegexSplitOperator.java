package edu.uci.ics.textdb.dataflow.regexsplit;

import edu.uci.ics.textdb.api.exception.TextDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;

/**
 * @author Qinhua Huang
 * @author Zuozhi Wang
 * 
 * This class is to divide an attribute by using a regex in order to get multiple tuples, 
 * with the other attributes unchanged.
 */

public class RegexSplitOperator extends AbstractSingleInputOperator implements ISourceOperator{
    
    private RegexSplitPredicate predicate;
    
    private List<ITuple> outputTupleBuffer;
    private int bufferCursor;
    private boolean hasBuffer;
    
    Schema inputSchema;
    
    
    public RegexSplitOperator(RegexSplitPredicate predicate) { // Attribute attribute
        
        this.predicate = predicate;
        this.bufferCursor = -1;
        this.hasBuffer = false;
    }
    
    @Override
    protected void setUp() throws DataFlowException {        
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
    }
    
    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
            return;
        }
        try {
            if (this.inputOperator == null) {
                throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
            }
            inputOperator.open();
            setUp();
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
        cursor = OPENED;
    }
    
    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    @Override
    protected ITuple computeNextMatchingTuple() throws TextDBException {
        ITuple inputTuple = null;
        ITuple resultTuple = null;

        // If buffer is false, fetch a input tuple to generate a buffer for output.
        if (hasBuffer == false) {
            inputTuple = inputOperator.getNextTuple();
            if (inputTuple == null) {
                return null;
            }
            populateOutputBuffer(inputTuple);
        }
        
        //if there is non-buffer and cursor < bufferSize, go ahead to get a output buffer.
        if (bufferCursor < outputTupleBuffer.size()) {
            resultTuple = outputTupleBuffer.get(bufferCursor);
            bufferCursor++;
            // if reached the end of buffer, reset the buffer properties.
            if (bufferCursor == outputTupleBuffer.size()) {
                hasBuffer = false;
                bufferCursor = -1;
            }
            return resultTuple;
        }
        return null;
    }
    
    
    public void populateOutputBuffer(ITuple inputTuple) throws TextDBException {        
        if (inputTuple != null)
        {
            String strToSplit;
            FieldType fieldType = this.inputSchema.getAttribute(predicate.getAttributeToSplit()).getFieldType();
            if (fieldType == FieldType.TEXT || fieldType == FieldType.STRING)
            {
                strToSplit = inputTuple.getField(predicate.getAttributeToSplit()).getValue().toString();
                List<String> splitTextList = getSplitText(strToSplit);
                outputTupleBuffer = new ArrayList<>();
                for (String splitText : splitTextList) {
                    List<IField> tupleFieldList = new ArrayList<>();
                    for (String attributeName : inputSchema.getAttributeNames()) {
                        if (attributeName.equals(predicate.getAttributeToSplit())) {
                            if (fieldType == FieldType.TEXT) {
                                tupleFieldList.add(new TextField(splitText));
                            } else {
                                tupleFieldList.add(new StringField(splitText));
                            }
                        } else {
                            tupleFieldList.add(inputTuple.getField(attributeName));
                        }
                    }
                    outputTupleBuffer.add(new DataTuple(inputSchema, tupleFieldList.stream().toArray(IField[]::new)));
                }
                hasBuffer = true;       //must has a buffer
                cursor = 0;
            }
        }
    }

    @Override
    protected void cleanUp() throws TextDBException {
        hasBuffer = false;
        bufferCursor = -1;
    }
    
    public RegexSplitPredicate getPredicate(){
        return this.predicate;
    }
    
    /*
     *  Get a Tuple list from input file operator.
     */
    public List<String> getSplitText(String strText) throws TextDBException {
        List<String> splitTextList = new ArrayList<>();
        //Create a pattern using regex.
        Pattern p = Pattern.compile(predicate.getRegex());
        
        // Match the pattern in the text.
        Matcher startM = p.matcher(strText);
        int startAt = 0;
        int count = 0;
        
        while(startM.find() || count == startM.groupCount()){
            count++;
            int endAt = startM.start();
            startAt = endAt;
            //Handling the string after the last matching group.
            if (count == startM.groupCount()){
                endAt = strText.length()-1;
            }
            splitTextList.add(strText.substring(startAt, endAt));
        }
        //Handling 
        return splitTextList;
    }

    @Override
    public ITuple processOneInputTuple(ITuple inputTuple) throws TextDBException {
        throw new RuntimeException("RegexSplit does not support process one tuple");
    }
}
