package edu.uci.ics.textdb.dataflow.split;
import edu.uci.ics.textdb.api.exception.TextDBException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;



/**
 * @author Qinhua Huang
 * @author Zuozhi Wang
 * 
 * This class handles tuples divide in a long text file source by using a regex.
 */
public class FileRegexSplitSourceOperator extends AbstractSingleInputOperator {
    private File file;
    private Scanner scanner;
    private Schema outputSchema;
    private int lineNum;
    private int tupleEndLineNum;
    private String splitRegex;
    private String nextTupleStartString;
    
    private boolean isOpen;
    
    public FileRegexSplitSourceOperator(String filePath, String Regex, Schema schema) { // Attribute attribute
        this.file = new File(filePath);
        this.outputSchema = schema;
        this.lineNum = 0;
        this.tupleEndLineNum = 0;
        this.splitRegex = Regex;
        this.nextTupleStartString = "";
        this.isOpen = false;
    }

    @Override
    public void open() throws TextDBException {
        if ( isOpen == false){
            try {
                this.scanner = new Scanner(file);
                isOpen = true;
            } catch (FileNotFoundException e) {
                throw new TextDBException("Failed to open FileSourceOperator", e);
            }
        }
    }

    @Override
<<<<<<< HEAD:textdb/textdb-dataflow/src/main/java/edu/uci/ics/textdb/dataflow/source/FileRegexSplitSourceOperator.java
/**
 * Construct a tuple with only one field schema.
 */
=======
>>>>>>> 2bef28db5b0cece0c694b2d4ec9dd2ad3e969410:textdb/textdb-dataflow/src/main/java/edu/uci/ics/textdb/dataflow/split/FileRegexSplitSourceOperator.java
    public ITuple getNextTuple() throws TextDBException {
        if ( isOpen == true && this.scanner.hasNextLine()) {
                try {
                    String tupleString = "";
                    tupleString = getTupleString();
                    
                    ITuple tuple = new DataTuple(outputSchema, new TextField(tupleString));
                    return tuple;
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                    return getNextTuple();
                }
            }
        return null;
    }

    @Override
    public void close() throws TextDBException {
        if ( isOpen == true ){
            if (this.scanner != null) {
                this.scanner.close();
            }
        }
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
<<<<<<< HEAD:textdb/textdb-dataflow/src/main/java/edu/uci/ics/textdb/dataflow/source/FileRegexSplitSourceOperator.java
    
/*
 * Split text into list tuples by a Regex line by line.
 */
    
    public String getTupleString(){
=======
       
    /*
     * Split text into list tuples by a Regex line by line.
     */
    private String getTupleString(){
>>>>>>> 2bef28db5b0cece0c694b2d4ec9dd2ad3e969410:textdb/textdb-dataflow/src/main/java/edu/uci/ics/textdb/dataflow/split/FileRegexSplitSourceOperator.java
        StringBuilder tupleString = new StringBuilder();
        tupleString.append(this.nextTupleStartString);
        if ( !this.nextTupleStartString.isEmpty() ){
            tupleString.append("\n");
            this.nextTupleStartString = "";
        }
        Pattern p = Pattern.compile(splitRegex);

        tupleEndLineNum = lineNum;
        boolean isTupleComplete = false;
        while (this.scanner.hasNextLine() && ! isTupleComplete ) {
            String line = scanner.nextLine();
            this.lineNum += 1;
            Matcher startM = p.matcher(line);
            
            if (startM.find()) {
                this.tupleEndLineNum = this.lineNum;
                this.nextTupleStartString = line;
                isTupleComplete = true;
            }else{
                tupleString.append(line+"\n");
            }
        }
        return tupleString.toString();
    }
}
