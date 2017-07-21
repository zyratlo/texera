package edu.uci.ics.textdb.exp.nlp.sentiment;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.constants.DataConstants.TextdbProject;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

public class NltkSentimentOperator implements IOperator {
    private final NltkSentimentOperatorPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    
    private List<Tuple> tupleBuffer;
    HashMap<String, String> idClassMap;
    
    private int cursor = CLOSED;
    
    private final static String PYTHON = "python3";
    private final static String PYTHONSCRIPT = Utils.getResourcePath("nltk_sentiment_classify.py", TextdbProject.TEXTDB_EXP);
    private final static String BatchedFiles = Utils.getResourcePath("id-text.csv", TextdbProject.TEXTDB_EXP);
    private final static String resultPath = Utils.getResourcePath("result-id-class.csv", TextdbProject.TEXTDB_EXP);
    
    private final static char SEPARATOR = ',';
    private final static char QUOTECHAR = '"';
    
    //Default nltk training model set to be "Senti.pickle"
    private String PicklePath = null;
    
    public NltkSentimentOperator(NltkSentimentOperatorPredicate predicate){
        this.predicate = predicate;
        
        String modelFileName = predicate.getInputAttributeModel();
        if (modelFileName == null)
            modelFileName = "NltkSentiment.pickle";
        
        this.PicklePath = Utils.getResourcePath(modelFileName, TextdbProject.TEXTDB_EXP);
        
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new RuntimeException("Cannot link this operator to another operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    /*
     * add a new field to the schema, with name resultAttributeName and type String
     */
    private Schema transformSchema(Schema inputSchema){
        if (inputSchema.containsField(predicate.getResultAttributeName())) {
            throw new RuntimeException(String.format(
                    "result attribute name %s is already in the original schema %s", 
                    predicate.getResultAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        return Utils.addAttributeToSchema(inputSchema, 
                new Attribute(predicate.getResultAttributeName(), AttributeType.STRING));
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
        
        // check if the input schema is presented
        if (! inputSchema.containsField(predicate.getInputAttributeName())) {
            throw new RuntimeException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        
        // check if the attribute type is valid
        AttributeType inputAttributeType = 
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
    
    private boolean computeTupleBuffer() {
        tupleBuffer = new ArrayList<Tuple>();
        //write [ID,text] to a CSV file.
        List<String[]> csvData = new ArrayList<>();
        int i = 0;
        Tuple inputTuple;
        while (i < predicate.getBatchSize()){
            if ((inputTuple = inputOperator.getNextTuple()) != null) {
                tupleBuffer.add(inputTuple);
                String[] idTextPair = new String[2];
                idTextPair[0] = inputTuple.getField(SchemaConstants._ID).getValue().toString();
                idTextPair[1] = inputTuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
                csvData.add(idTextPair);
                i++;
            } else {
                break;
            }
        }
        if (tupleBuffer.isEmpty()) {
            return false;
        }
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(BatchedFiles));
            writer.writeAll(csvData);
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new DataFlowException(e.getMessage(), e);
        }
        return true;
    }
    
    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            return null;
        }
        if (tupleBuffer == null){
            if (computeTupleBuffer() == true) {
                computeClassLabel(BatchedFiles);
            } else {
                return null;
            }
        }
        return populateBufferTuple();
    }
    
    // Process the data file using NLTK
    private String computeClassLabel(String filePath) {
	    try{
            /*
             *  In order to use the NLTK package to do classification, we start a
             *  new process to run the package, and wait for the result of running
             *  the process as the class label of this text field.
             *  Python call format:
             *      #python3 nltk_sentiment_classify picklePath dataPath resultPath
             * */
            List<String> args = new ArrayList<String>(
                    Arrays.asList(PYTHON, PYTHONSCRIPT, PicklePath, filePath, resultPath));
            ProcessBuilder processBuilder = new ProcessBuilder(args);
            
            Process p = processBuilder.start();
            p.waitFor();
            
            //Read label result from file generated by Python.
            CSVReader csvReader = new CSVReader(new FileReader(resultPath), SEPARATOR, QUOTECHAR, 1);
            List<String[]> allRows = csvReader.readAll();
               
            idClassMap = new HashMap<String, String>();
            //Read CSV line by line
            for(String[] row : allRows){
                idClassMap.put(row[0], row[1]);
            }
            csvReader.close();
        }catch(Exception e){
            throw new DataFlowException(e.getMessage(), e);
        }
        return null;
    }
    
    private Tuple populateBufferTuple() {
        Tuple outputTuple = tupleBuffer.get(0);
        tupleBuffer.remove(0);
        if (tupleBuffer.isEmpty()) {
            tupleBuffer = null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(outputTuple.getFields());
        
        String className = idClassMap.get(outputTuple.getField(SchemaConstants._ID).getValue().toString());
        outputFields.add(new TextField( className ));
        return new Tuple(outputSchema, outputFields);
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

