package edu.uci.ics.textdb.api.storage;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.Tuple;
import edu.uci.ics.textdb.api.exception.TextDBException;

public interface IDataWriter {
    
    public IField insertTuple(Tuple tuple) throws TextDBException;

    public void clearData() throws TextDBException;
}
