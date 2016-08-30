package edu.uci.ics.textdb.api.dataflow;

public interface IConnector {
    
    int getOutputNumber();
    
    IOperator getOutputOperator(int outputIndex);
    
}
