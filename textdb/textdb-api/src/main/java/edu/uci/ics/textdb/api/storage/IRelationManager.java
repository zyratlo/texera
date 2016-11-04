package edu.uci.ics.textdb.api.storage;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;

/**
 * IRelationManager is the interface for TextDB's relation manager, 
 *   which manages the meta data information about each table, 
 *   and provides functions to manipulate tables.
 * 
 * @author Zuozhi Wang
 */
public interface IRelationManager {
    
    public IRelationManager getRelationManager();
    
    public boolean checkTableExistence(String tableName);
    
    // create a new table, tableName must be unique
    public void createTable(String tableName, String indexDirectory, Schema schema) throws Exception;
    
    // drop a table
    public void deleteTable(String tableName) throws Exception;
    
    // insert a tuple to a table, the primaryAttribute field must be unique
    public void insertTuple(String tableName, ITuple tuple, String primaryAttribute) throws Exception;
    
    // delete a tuple by its primary attribute
    public void deleteTuple(String tableName, String primaryAttribute, Object value) throws Exception;
    
    // update a tuple by its primary attribute
    public void updateTuple(String tableName, ITuple tuple, String primaryAttribute, Object value) throws Exception;
    
    // get a tuple by its primary attribute
    public ITuple getTuple(String tableName, String primaryAttribute, Object value) throws Exception;

}
