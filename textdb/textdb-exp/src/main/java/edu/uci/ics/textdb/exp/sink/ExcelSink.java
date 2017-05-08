package edu.uci.ics.textdb.exp.sink;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.constants.DataConstants.TextdbProject;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

/**
 * ExcelSink is a sink that can write a list of tuples into an excel file
 * setInputOperator -> open -> collectAllTuples -> close -> deleteFile
 * The path of saved files is "textdb/textdb/textdb-perftest/src/main/resources/index/excel/"
 * @author Jinggang Diao
 *
 */
public class ExcelSink implements ISink {
    
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;
    
    private boolean isOpen;
    
    private Workbook wb;
    private FileOutputStream fileOut;
    private Sheet sheet;
    private int rowCursor;
    
    private static String excelIndexDirectory = Utils.getResourcePath("/index/excel/", TextdbProject.TEXTDB_EXP) + "/";
    private String fileName;

    
    public ExcelSink() {
    	isOpen = false;
    	rowCursor = 0;
    }
    
    public boolean getIsOpen(){
    	return isOpen;
    }
    
    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }
    
    public IOperator getInputOperator() {
        return this.inputOperator;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void open() throws TextDBException{
        if (isOpen) {
            return;
        }     
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = Utils.removeAttributeFromSchema(inputSchema, SchemaConstants._ID, SchemaConstants.PAYLOAD);
        wb = new XSSFWorkbook();
        DateFormat df = new SimpleDateFormat("YYYYMMDD_HH_mm_ss_SSSS");
        Date dateobj = new Date();
        fileName = df.format(dateobj) + ".xlsx";
    	try {
			fileOut = new FileOutputStream(excelIndexDirectory + fileName);
		} catch (FileNotFoundException e) {
			throw new DataFlowException("Fail to find the file: "+excelIndexDirectory + fileName, e);
		}
    	sheet = wb.createSheet("new sheet");
    	Row row = sheet.createRow(rowCursor++);
    	List<String> attributeNames = outputSchema.getAttributeNames();
    	for(int i = 0; i < attributeNames.size(); i++){
    		String attributeName = attributeNames.get(i);
        	row.createCell(i).setCellValue(attributeName);
    	}
        isOpen = true;
    }

    @Override
    public void processTuples() throws TextDBException {
        return;
    }
    
    /**
     * getNextTuple() will return the cleaned tuple from previous inputOperator.
     * At the same time, it creates a new row in excel and writes the cleaned tuple to that row.
     * List field is kept as one single string.
     */
    @Override
    public Tuple getNextTuple() throws TextDBException {
    	Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }
        Tuple tuple = Utils.removeFields(inputTuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);
    	Row row = sheet.createRow(rowCursor++);
    	List<IField> fields = tuple.getFields();
    	for(int i = 0; i < fields.size(); i++){
    		this.writeCell(row.createCell(i), fields.get(i));
    	}
        return tuple;
    }

    @Override
    public void close() throws TextDBException {
        if (isOpen) {
        	inputOperator.close();
            try {
                wb.write(fileOut);
    			fileOut.close();
                isOpen = false; 
    		} catch (IOException e) {
            	System.out.println("ExcelSink::open  "+e.getMessage());
    		}
        }
    }

    
    /**
     * Collects ALL the tuples to an in-memory list
     * also add all tuples into the excel file by calling this.getNextTuple
     * 
     * @return a list of tuples
     * @throws TextDBException
     */
    public List<Tuple> collectAllTuples() throws TextDBException {
        ArrayList<Tuple> results = new ArrayList<>();
        Tuple tuple;
        while ((tuple = this.getNextTuple()) != null) {
            results.add(Utils.removeFields(tuple, SchemaConstants._ID, SchemaConstants.PAYLOAD));
        }
        return results;
    }

    /**
     * delete the file created by ExcelSink operator
     * @throws FileNotFoundException
     */
    public void deleteFile(){
    	if(isOpen){
    		close();
    	}
    	File file = new File(excelIndexDirectory + fileName);
    	file.delete();
    }
    
    /*
     * Write a field into a cell base on the field type.
     * 		Excel-Double <-> DoubleField, IntegerField
     * 		Excel-Date 	<-> DateField
     * 		Excel-String <-> All other fields, including ListField
     * Special case: a null field will be written as an empty string
     */
    private void writeCell(Cell cell, IField field){
    	if(field == null){
    		cell.setCellValue("");
    		return;
    	}
    	Object fieldClass = field.getClass();
    	if(fieldClass instanceof DoubleField || fieldClass instanceof IntegerField){
    		cell.setCellValue((double) field.getValue());
    	}
    	else if(fieldClass instanceof DateField){
    		cell.setCellValue((Date)field.getValue());
    	}
    	else{
    		cell.setCellValue(field.getValue().toString());
    	}
    }
    
}
