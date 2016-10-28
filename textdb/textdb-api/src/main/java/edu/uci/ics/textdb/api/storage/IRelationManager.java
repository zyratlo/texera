package edu.uci.ics.textdb.api.storage;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;

public interface IRelationManager {
    
    public IRelationManager getRelationManager();
    
    public void createTable(String tableName, String indexDirectory, Schema schema) throws Exception;
    
    public void deleteTable(String tableName) throws Exception;
    
    public void insertTuple(String tableName, ITuple tuple) throws Exception;
    
    public void deleteTuple(String tableName, String primaryAttribute, Object primaryValue) throws Exception;
    
    public void updateTuple(String tableName, ITuple tuple, String primaryAttribute, Object primaryValue) throws Exception;
    
    public ITuple getTuple(String tableName, String primaryAttribute, Object primaryValue) throws Exception;

}
