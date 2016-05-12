package edu.uci.ics.textdb.api.dataflow;

/**
 * Created by chenli on 5/11/16.
 */
public interface ISink {
    void open() throws Exception;

    void processTuples() throws Exception;

    void close() throws Exception;
}
