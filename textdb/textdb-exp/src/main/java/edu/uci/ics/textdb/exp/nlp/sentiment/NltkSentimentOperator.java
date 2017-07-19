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
    
    private static String PYTHON = "python3";
    public static String PYTHONSCRIPT = Utils.getResourcePath("nltk_sentiment_classify.py", TextdbProject.TEXTDB_EXP);
    public static String BatchedFiles = Utils.getResourcePath("id-text.csv", TextdbProject.TEXTDB_EXP);
    public static String PicklePath = Utils.getResourcePath("Senti.pickle", TextdbProject.TEXTDB_EXP);
    public static String resultPath = Utils.getResourcePath("result-id-class.csv", TextdbProject.TEXTDB_EXP);
    
    public static char SEPARATOR = ',';
    public static char QUOTECHAR = '"';
    
    public NltkSentimentOperator(NltkSentimentOperatorPredicate predicate){
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new RuntimeException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    /*
     * adds a new field to the schema, with name resultAttributeName and type String
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
        
        // check if input schema is presented
        if (! inputSchema.containsField(predicate.getInputAttributeName())) {
            throw new RuntimeException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema.getAttributeNames()));
        }
        
        // check if attribute type is valid
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
	
	// This will be deleted after review.
	public static void main(String[] args2) throws Exception{
	    CSVWriter writer = new CSVWriter(new FileWriter(BatchedFiles));
	    List<String[]> csvData = new ArrayList<>(); 
	    
	    String[] entrites = new String[2];
	    entrites[0] = "3NNNNNNNN";
	    entrites[1] =  "I don't think so.";
	    csvData.add(entrites);
	    
	    String[] entrites2 = new String[2];
	    entrites2[0] = "6xxxxx";
        entrites2[1] =  "You are so bad.";
        csvData.add(entrites2);
        
	    writer.writeAll(csvData);
	    writer.close();
	    
	    // call format:
	    // python3 classifier-loader picklePath dataPath resultPath
	    List<String> args = new ArrayList<String>(Arrays.asList(PYTHON, PYTHONSCRIPT, PicklePath, BatchedFiles, resultPath));
//        args.addAll(new ArrayList<String>(Arrays.asList(text)));
	    System.out.println(args.toString());
	    ProcessBuilder pb = new ProcessBuilder(args);
        
        Process p = pb.start();
        p.waitFor();
	    
	  //Build reader instance
	    CSVReader reader = new CSVReader(new FileReader(resultPath), SEPARATOR, QUOTECHAR, 1);
	       
	      //Read all rows at once
	    List<String[]> allRows = reader.readAll();
	       
	      //Read CSV line by line and use the string array as you want
	    for(String[] row : allRows){
//	       System.out.println(Arrays.toString(row));
	       System.out.println(row[0]+"=="+row[1]);
//	       System.out.println(Arrays.toString(row).toString().split(", ")[0]+"<-->"+Arrays.toString(row).toString().split(",")[1]);
	    }
	}
	@Override
	public Tuple getNextTuple() throws TextDBException {
	    Tuple tuple;
	    if (tupleBuffer == null){
	        tupleBuffer = new ArrayList<Tuple>();
	        //write [ID,text] to a CSV file.
	        List<String[]> csvData = new ArrayList<>();
	        
	        int i = 0;
	        while (i < predicate.getSizeTupleBuffer()){
        	    if ((tuple = inputOperator.getNextTuple()) != null) {
        	        tupleBuffer.add(tuple);
        	        String[] idText = new String[2];
        	        idText[0] = tuple.getField(SchemaConstants._ID).getValue().toString();
        	        idText[1] = tuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
        	        csvData.add(idText);
        	        i++;
        	    } else {
        	        break;
        	    }
    	    }
    	    if (tupleBuffer.isEmpty()) {
    	        return null;
    	    }
    	    try {
    	        CSVWriter writer = new CSVWriter(new FileWriter(BatchedFiles));
    	        writer.writeAll(csvData);
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
    	    computeClassLabel(BatchedFiles);
	    }
	    
	    if (cursor == CLOSED) {
            return null;
        }
	    
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
	
	// Processing data file using python3
	private String computeClassLabel(String filePath) {
	    try{
            /*
             *  In order to use the NLTK package to do classification, we start a
             *  new process to run the package, and wait for the result of running
             *  the process as the class label of this text field.
             *  Python call format:
             *      #python3 classifier-loader picklePath dataPath resultPath
             * */
            List<String> args = new ArrayList<String>(
                    Arrays.asList(PYTHON, PYTHONSCRIPT, PicklePath, filePath, resultPath));
            ProcessBuilder pb = new ProcessBuilder(args);
            
            Process p = pb.start();
            p.waitFor();
            
            //Read label result from file generated by Python.
            CSVReader reader = new CSVReader(new FileReader(resultPath), SEPARATOR, QUOTECHAR, 1);
            List<String[]> allRows = reader.readAll();
               
            idClassMap = new HashMap<String, String>();
            //Read CSV line by line
            for(String[] row : allRows){
                idClassMap.put(row[0], row[1]);
            }
            reader.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
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
