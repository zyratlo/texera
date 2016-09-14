package edu.uci.ics.textdb.dataflow.sink;

import java.io.File;
import java.io.PrintWriter;

import edu.uci.ics.textdb.api.common.ITuple;

/**
 * Created by chenli on 5/11/16.
 *
 * This class serializes each tuple from the subtree to a given file.
 */
public class FileSink extends AbstractSink {
    
    @FunctionalInterface
    public static interface TupleToString {
        String convertToString(ITuple tuple);
    }

    private PrintWriter printWriter;
    private final File file;   
    private TupleToString toStringFunction = (tuple -> tuple.toString());
    
    public FileSink(File file) {
        this.file = file;
    }
    
    public void setToStringFunction(TupleToString toStringFunction) {
        this.toStringFunction = toStringFunction;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.printWriter = new PrintWriter(file);
    }

    @Override
    public void close() throws Exception {
        if (this.printWriter != null) {
            this.printWriter.close();
        }
        super.close();
    }

    @Override
    protected void processOneTuple(ITuple nextTuple) {
        printWriter.write(toStringFunction.convertToString(nextTuple));
    }
    
    public File getFile() {
        return this.file;
    }
}
