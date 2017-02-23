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
 * This class is to divide an attribute of TextField or StringField by using a regex in order to get multiple tuples, 
 * with the other attributes unchanged.
 * 
 * Example:
 * Source text: "Make America be Great America Again."
 * splitRegex = "America"
 * Split output can be 1 of the following 3, in terms of the SplitTypes:
 * 1. GROUP_RIGHT: ["Make ", "America be Great ", "America Again."]
 * 2. GROUP_LEFT: ["Make America", " be Great America"," Again."]
 * 3. STANDALONE: ["Make ", "America", " be Great ", "America", " Again."]
 * 
 */

public class RegexSplitOperator extends AbstractSingleInputOperator implements ISourceOperator{

    private RegexSplitPredicate predicate;

    private List<ITuple> outputTupleBuffer;
    private int bufferCursor;
    private boolean hasOutputBufferTuple;
    private Schema inputSchema;

    public RegexSplitOperator(RegexSplitPredicate predicate) {
        this.predicate = predicate;
        this.bufferCursor = 0;
        this.hasOutputBufferTuple = false;
    }

    @Override
    protected void setUp() throws DataFlowException {        
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
    }

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
        
        // If buffer is empty, fetch a input tuple to generate a buffer for output.
        if (hasOutputBufferTuple == false) {
            inputTuple = inputOperator.getNextTuple();
            if (inputTuple == null) {
                return null;
            }
            populateOutputBuffer(inputTuple);
        }
        
        //if there is a tuples buffer and cursor < bufferSize, go ahead to get a output buffer.
        if (bufferCursor < outputTupleBuffer.size()) {
            resultTuple = outputTupleBuffer.get(bufferCursor);
            bufferCursor++;
            // if reached the end of buffer, reset the buffer cursor.
            if (bufferCursor == outputTupleBuffer.size()) {
                hasOutputBufferTuple = false;
                bufferCursor = 0;
            }
            return resultTuple;
        }
        return null;
    }

    private void populateOutputBuffer(ITuple inputTuple) throws TextDBException {        
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
                hasOutputBufferTuple = true;       //must has a buffer
                bufferCursor = 0;
            }
        }
    }

    @Override
    protected void cleanUp() throws TextDBException {
        hasOutputBufferTuple = false;
        bufferCursor = 0;
    }
    
    public RegexSplitPredicate getPredicate() {
        return this.predicate;
    }
    
    /*
     *  Get a Tuple list from input file operator.
     */
    private List<String> getSplitText(String strText) throws TextDBException {
        List<String> splitTextList = new ArrayList<>();
        //Create a pattern using regex.
        Pattern p = Pattern.compile(predicate.getRegex());
        
        // Match the pattern in the text.
        Matcher regexMatcher = p.matcher(strText);
        List<Integer> splitIndex = new ArrayList<Integer>();
        splitIndex.add(0);

        while(regexMatcher.find()){
            if (predicate.getSplitType() == RegexSplitPredicate.SplitType.GROUP_RIGHT) {
                splitIndex.add(regexMatcher.start());
            } else if (predicate.getSplitType() == RegexSplitPredicate.SplitType.GROUP_LEFT) {
                splitIndex.add(regexMatcher.end());
            } else if (predicate.getSplitType() == RegexSplitPredicate.SplitType.STANDALONE) {
                splitIndex.add(regexMatcher.start());
                splitIndex.add(regexMatcher.end());
            }
        }
        splitIndex.add(strText.length());
        
        for (int i = 0 ; i < splitIndex.size() - 1; i++) {
            if (splitIndex.get(i) != splitIndex.get(i+1)) {
                splitTextList.add(strText.substring(splitIndex.get(i), splitIndex.get(i + 1)));
            }
        }
        
        return splitTextList;
    }
    
    @Override
    public ITuple processOneInputTuple(ITuple inputTuple) throws TextDBException {
        throw new RuntimeException("RegexSplit does not support process one tuple");
    }
}
