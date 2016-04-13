package edu.uci.ics.textdb.dataflow.source;

import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IDataReader;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.dataflow.LuceneDataReader;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;

/**
 * Created by chenli on 3/28/16.
 */
public class ScanBasedSourceOperator implements ISourceOperator {
    
    private IDataReader dataReader;
    
    public ScanBasedSourceOperator(String dataDirectory, List<Attribute> schema) throws DataFlowException {
        if(schema == null || schema.isEmpty()){
            throw new DataFlowException(ErrorMessages.SCHEMA_CANNOT_BE_NULL);
        }
        dataReader = new LuceneDataReader(dataDirectory, schema, 
                LuceneConstants.SCAN_QUERY,schema.get(0).getFieldName());
    }

    @Override
    public void open() throws DataFlowException {
        try {
            dataReader.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
        try {
            return dataReader.getNextTuple();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws DataFlowException {
        try {
            dataReader.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
