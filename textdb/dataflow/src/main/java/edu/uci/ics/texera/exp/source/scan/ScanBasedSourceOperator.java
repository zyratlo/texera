package edu.uci.ics.texera.exp.source.scan;

import edu.uci.ics.texera.api.exception.DataFlowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TextDBException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.storage.RelationManager;

/**
 * Created by chenli on 3/28/16.
 */
public class ScanBasedSourceOperator implements ISourceOperator {

    private DataReader dataReader;
    
    private boolean isOpen = false;

    public ScanBasedSourceOperator(ScanSourcePredicate predicate) throws DataFlowException {
        try {
            this.dataReader = RelationManager.getRelationManager().getTableDataReader(
                    predicate.getTableName(), new MatchAllDocsQuery());
            // TODO add an option to set if payload is added in the future.
            this.dataReader.setPayloadAdded(true);
        } catch (StorageException e) {
            throw new DataFlowException(e);
        }
    }

    @Override
    public void open() throws TextDBException {
        if (isOpen) {
            return;
        }
        try {
            dataReader.open();
            isOpen = true;
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (! isOpen) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            return dataReader.getNextTuple();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws TextDBException {
        if (! isOpen) {
            return;
        }
        try {
            dataReader.close();
            isOpen = false;
        } catch (Exception e) {
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    @Override
    public Schema getOutputSchema() {
        return dataReader.getOutputSchema();
    }
}
