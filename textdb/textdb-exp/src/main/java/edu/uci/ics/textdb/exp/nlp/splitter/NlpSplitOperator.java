package edu.uci.ics.textdb.exp.nlp.splitter;

import java.util.ArrayList;
import java.util.List;
import java.io.Reader;
import java.io.StringReader;

import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

/**
 * This Operator splits the input string using Stanford core NLP's ssplit annotation.
 * 
 * The result is an list of sentences.
 * 
 * The result will be put into an attribute with resultAttributeName specified in predicate, and type List<String>.
 * 
 * @author Venkata Raj Kiran Kollimarla, Vinay Bagade
 *
 */
public class NlpSplitOperator implements IOperator {
    
    private final NlpSplitPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    //A flag to keep track of any remaining sentences from the previous input tuple
//    private boolean sentenceListIsEmpty = true;
    //A tuple that persists between method calls
    private Tuple currentTuple;
    //A list of sentences generated from the current tuple
    private List<TextField> currentSentenceList = new ArrayList<TextField>();
    
    
    public NlpSplitOperator(NlpSplitPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new RuntimeException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    /*
     * adds a new field to the schema, with name resultAttributeName and type list of strings
     */
    private Schema transformSchema(Schema inputSchema) {
        if (inputSchema.containsField(predicate.getResultAttributeName())) {
            throw new RuntimeException(String.format(
                    "result attribute name %s is already in the original schema %s", 
                    predicate.getResultAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        return Utils.addAttributeToSchema(inputSchema, 
                new Attribute(predicate.getResultAttributeName(), AttributeType.LIST));
    }
      

    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();
        
        // check if input schema is present
        if (! inputSchema.containsField(predicate.getInputAttributeName())) {
            throw new RuntimeException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        
        // check if attribute type is valid
        AttributeType inputAttributeType = 
                //Does predicate give the correct input attribute name?
                inputSchema.getAttribute(predicate.getInputAttributeName()).getAttributeType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) || 
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new RuntimeException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getInputAttributeName(),
                    inputAttributeType));
        }
        
        // generate output schema by transforming the input schema
        outputSchema = transformSchema(inputOperator.getOutputSchema());
        
        cursor = OPENED;

    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            return null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        
        if(predicate.getOutputType() == NLPOutputType.ONE_TO_ONE) {
            currentTuple = inputOperator.getNextTuple();
            if (currentTuple == null) {
                return null;
            }
            currentSentenceList = getSentenceList(currentTuple);
            outputFields.addAll(currentTuple.getFields());
            outputFields.add(new ListField<TextField>(currentSentenceList));
        }
        
        else if (predicate.getOutputType() == NLPOutputType.ONE_TO_MANY) {
            if(currentSentenceList.isEmpty()) {
                currentTuple = inputOperator.getNextTuple();
                if (currentTuple == null) return null;
                currentSentenceList = getSentenceList(currentTuple);
            }
            
            outputFields.addAll(currentTuple.getFields());
            //Add the sentences from the current sentence list one by one in the order in which
            //they were generated in the getSentenceList function
            outputFields.add(currentSentenceList.remove(0)); //Append a TextField to the output tuple    
        }
        
        return new Tuple(outputSchema, outputFields);
        
    }
    
    
    private List<TextField> getSentenceList(Tuple inputTuple) {
        
        String inputText = inputTuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
        Reader reader = new StringReader(inputText);
        DocumentPreprocessor dp = new DocumentPreprocessor(reader);
        List<TextField> sentenceList = new ArrayList<TextField>();
        
        for (List<HasWord> sentence : dp) {
            TextField sentenceText = new TextField(Sentence.listToString(sentence));
            sentenceList.add(sentenceText);
         }
        
        return sentenceList;
    }
    

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
            return;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }
}
