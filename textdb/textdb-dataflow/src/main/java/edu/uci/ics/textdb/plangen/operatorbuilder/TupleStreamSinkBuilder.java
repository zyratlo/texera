package edu.uci.ics.textdb.plangen.operatorbuilder;

import java.util.Map;

import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;

public class TupleStreamSinkBuilder {
    
    public static TupleStreamSink buildTupleStreamSink(Map<String, String> operatorProperties) {
        return new TupleStreamSink();
    }

}
