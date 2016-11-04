package edu.uci.ics.textdb.relation;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IRelationManager;

public class RelationManager implements IRelationManager {

    @Override
    public IRelationManager getRelationManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean checkTableExistence(String tableName) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void createTable(String tableName, String indexDirectory, Schema schema, String luceneAnalyzer) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteTable(String tableName) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void insertTuple(String tableName, ITuple tuple, String primaryAttribute) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteTuple(String tableName, String primaryAttribute, Object value) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateTuple(String tableName, ITuple tuple, String primaryAttribute, Object value) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ITuple getTuple(String tableName, String primaryAttribute, Object value) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IDataReader scanTable(String tableName) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTableDirectory(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Schema getTableSchema(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTableAnalyzer(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

}
