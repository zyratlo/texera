package edu.uci.ics.textdb.dataflow.regexsplit;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.storage.relation.RelationManager;

/**
 * @author Qinhua Huang
 * @author Zuozhi Wang
 * 
 * This class is to divide an attribute by using a regex in order to get multiple tuples, 
 * with the other attributes unchanged.
 */

public class RegexSplitOperator extends AbstractSingleInputOperator implements ISourceOperator{
    
    private static RegexSplitPredicate predicate;
    private String attrToSplit;
    private ITuple inputTuple;
    
    private List<ITuple> outputTupleBuffer;
    private int bufferCursor;
    private int bufferSize;
    private boolean hasBuffer;
    
    private Schema outputSchema;
    private String splitRegex;
    private int cursor = CLOSED;
    
    private int resultCursor = -1;
    private int limit = Integer.MAX_VALUE;
    private int offset = 0;
    
    Schema inputSchema;
    
    private boolean isOpen;
    
    public RegexSplitOperator(RegexSplitPredicate predicate) { // Attribute attribute
        
        this.predicate = predicate;
        attrToSplit = predicate.getAttributeToSplit();
        splitRegex = this.getPredicate().getRegex();
        this.bufferCursor = -1;
        this.bufferSize = 0;
        this.hasBuffer = false;
    }
    
    @Override
    protected void setUp() throws DataFlowException {        
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.SPAN_LIST_ATTRIBUTE);
        }
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
    
    //Do we still need this close() function?
    /*
    @Override
    public void close() throws TextDBException {
        if ( isOpen == true ){
            isOpen = false;
            cursor = CLOSED;
            inputOperator.close();
        }
    }
   */ 
    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    @Override
    protected ITuple computeNextMatchingTuple() throws TextDBException {
        ITuple inputTuple = null;
        ITuple resultTuple = null;

        // If buffer is false, fetch a input tuple to generate a buffer for output.
        if (hasBuffer == false)
        {
            inputTuple = inputOperator.getNextTuple();
            // if no next buffer available, return null;
            if ( processOneInputTuple(inputTuple) == null){
                return null;
            }
        }
        
        //if there is non-buffer and cursor < bufferSize, go ahead to get a output buffer.
        if ( bufferCursor < bufferSize ){
            resultTuple = outputTupleBuffer.get(bufferCursor);
            bufferCursor++;
            // if reached the end of buffer, reset the buffer properties.
            if (bufferCursor == bufferSize){
                hasBuffer = false;
                bufferCursor = -1;
                bufferSize = 0;
            }
            return resultTuple;
        }
        return null;
    }
    
    @Override
    public ITuple processOneInputTuple(ITuple inputTuple) throws TextDBException {
        // TODO Auto-generated method stub
        
        if (inputTuple != null)
        {
            String strToSplit;
            FieldType fieldType = this.inputSchema.getAttribute(attrToSplit).getFieldType();
            if (fieldType == FieldType.TEXT || fieldType == FieldType.STRING)
            {
                strToSplit = inputTuple.getField(attrToSplit).getValue().toString();
                outputTupleBuffer = getTuplesBuffer(strToSplit);
                hasBuffer = true;       //must has a buffer
                bufferSize = outputTupleBuffer.size(); //must be a value no less than 1
                cursor = 0;
                return outputTupleBuffer.get(1);
            }
        }
        return null;
    }

    @Override
    protected void cleanUp() throws TextDBException {
        // TODO Auto-generated method stub
        hasBuffer = false;
        bufferCursor = -1;
        bufferSize = 0;
    }
    
    public RegexSplitPredicate getPredicate(){
        return this.predicate;
    }
    /*
     *  Get a Tuple list from input file operator.
     */
    public List<ITuple> getTuplesBuffer(String strText) throws TextDBException {
        //Create a pattern using regex.
        Pattern p = Pattern.compile(splitRegex);
        
        // Match the pattern in the text.
        Matcher startM = p.matcher(strText);
        int startAt = 0;
        int count = 0;
        String tmpStr;
        while(startM.find() || count == startM.groupCount()){
            count++;
            int endAt = startM.start();
            startAt = endAt;
            //Handling the string after the last matching group.
            if (count == startM.groupCount()){
                endAt = strText.length()-1;
            }
            tmpStr = strText.substring(startAt, endAt);
            ITuple tuple = null;
            
            List<IField> fieldList = null;
            //construct a tuple by producing each attributes.
            //This needs to be made clear.
            for (String fieldName : this.predicate.getAttributeNames()) {
                FieldType fieldType = this.inputSchema.getAttribute(fieldName).getFieldType();
                String fieldValue;
                
                //logic:
                /*
                 * if  (fieldName == attrToSplit){
                    fieldValue = tmpStr; 
                } else{
                    copy original field(Value and type)
                }
                 */
                
                if (fieldName == attrToSplit){
                    fieldValue = tmpStr; 
                } else{
                    fieldValue = inputTuple.getField(fieldName).getValue().toString();
                }
                if (fieldType == fieldType.t)
                fieldList.add(TextField(fieldValue));
                fieldList.add(fieldValue);
            }
            //Construct the buffer with fieldList
            tuple = new DataTuple(this.inputSchema, fieldList.stream().toArray(IField[]::new));
            //Add tuple to output TupleBuffer.
            if (tuple != null){
                this.outputTupleBuffer.add(tuple);
            }
        }
        //Handling 
        return outputTupleBuffer;
    }
}
