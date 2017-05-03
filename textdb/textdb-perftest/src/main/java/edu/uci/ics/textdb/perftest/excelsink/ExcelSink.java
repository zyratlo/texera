package edu.uci.ics.textdb.perftest.sink;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

/**
 * ExcelSink is a sink that can write a list of tuples into an excel file
 * setInputOperator -> open -> collectAllTuples -> close
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
    Sheet sheet;
    Row row;
    
    private String fileName;

    
    public ExcelSink() {
    	isOpen = false;
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
        try{
            wb = new XSSFWorkbook();
            DateFormat df = new SimpleDateFormat("YYYYMMDD_HH_mm_ss_SSSS");
            Date dateobj = new Date();
            fileName = df.format(dateobj) + ".xlsx";
        	fileOut = new FileOutputStream(fileName);
        	sheet = wb.createSheet("new sheet");
        	row = sheet.createRow((short)0);
        	List<String> attributeNames = outputSchema.getAttributeNames();
        	//TODO::if attribute is a list, we cannot give it a fixed column size
        	for(int i = 0; i < attributeNames.size(); i++){
        		String attributeName = attributeNames.get(i);
            	row.createCell(i).setCellValue(attributeName);
        	}
            isOpen = true;
        }catch(Exception e){
        	System.out.println("ExcelSink::open  "+e.getMessage());
        }
    }

    @Override
    public void processTuples() throws TextDBException {
        return;
    }
    
    @Override
    public Tuple getNextTuple() throws TextDBException {
        Tuple tuple = inputOperator.getNextTuple();
        if (tuple == null) {
            return null;
        }
        return Utils.removeFields(tuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);
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
     * Collects ALL the tuples to an in-memory list, but also add all tuples into the excel file
     * 
     * @return a list of tuples
     * @throws TextDBException
     */
    public List<Tuple> collectAllTuples() throws TextDBException {
        ArrayList<Tuple> results = new ArrayList<>();
        Tuple tuple;
        while ((tuple = inputOperator.getNextTuple()) != null) {
            results.add(Utils.removeFields(tuple, SchemaConstants._ID, SchemaConstants.PAYLOAD));
        }
        int count = 1;
        for(Tuple tp: results){
        	row = sheet.createRow(count++);
        	List<IField> fields = tp.getFields();
        	for(int i = 0; i < fields.size(); i++){
        		row.createCell(i).setCellValue(fields.get(i).getValue().toString());
        	}
        }
        return results;
    }

    /**
     * delete the file created by ExcelSink operator
     * @throws FileNotFoundException
     */
    public void deleteFile() throws FileNotFoundException{
    	if(isOpen){
    		close();
    	}
    	File file = new File(fileName);
    	file.delete();
    }
    
}
