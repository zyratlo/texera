package edu.uci.ics.textdb.api.storage;

import edu.uci.ics.textdb.api.common.Schema;

/**
 * 
 * @author sandeepreddy602
 *
 */
public interface IDataStore {
    public String getDataDirectory();

    public Schema getSchema();
}
