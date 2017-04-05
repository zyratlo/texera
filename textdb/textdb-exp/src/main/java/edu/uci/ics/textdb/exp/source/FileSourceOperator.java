package edu.uci.ics.textdb.exp.source;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

/**
 * FileSourceOperator treats files on disk as a source. FileSourceOperator reads
 * a file line by line. A user needs to provide a custom function to convert a
 * string to tuple.
 * 
 * @author zuozhi
 */
public class FileSourceOperator implements ISourceOperator {

    private final FileSourcePredicate predicate;
    // a list of files, 
    private final List<File> fileList;
    // filter non-exist files, directories and file that start with "."
    private static final FileFilter fileFilter = 
            (f -> f.exists() && (!f.isDirectory()) && (! f.getName().startsWith(".")));

    public FileSourceOperator(FileSourcePredicate predicate) {
        this.predicate = predicate;
        File file = new File(predicate.getFilePath());
        if (! file.exists()) {
            throw new RuntimeException(String.format("file %s doesn't exist", predicate.getFilePath()));
        }
        this.fileList = new ArrayList<>();
        if (file.isDirectory()) {
            for (File subFile : Arrays.asList(file.listFiles(fileFilter))) {
                
            }
        } else {
            
        }
    }

    @Override
    public void open() throws TextDBException {
        try {
            this.scanner = new Scanner(file);
        } catch (FileNotFoundException e) {
            throw new TextDBException("Failed to open FileSourceOperator", e);
        }
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (scanner.hasNextLine()) {
            try {
                return this.toTupleFunc.convertToTuple(scanner.nextLine());
            } catch (Exception e) {
                e.printStackTrace(System.err);
                return getNextTuple();
            }
        }
        return null;
    }

    @Override
    public void close() throws TextDBException {
        if (this.scanner != null) {
            this.scanner.close();
        }
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

}
