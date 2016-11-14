package edu.uci.ics.textdb.storage.relation;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IRelationManager;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class RelationManager implements IRelationManager {
    
    private static volatile RelationManager singletonRelationManager = null;
    
    private RelationManager() throws StorageException {
        if (! catalogExists()) {
            initializeCollectionCatalog();
            initializeSchemaCatalog();
        }
    }

    @Override
    public IRelationManager getRelationManager() throws StorageException {
        if (singletonRelationManager == null) {
            synchronized (RelationManager.class) {
                if (singletonRelationManager == null) {
                    singletonRelationManager = new RelationManager();
                }
            }
        }
        return singletonRelationManager;
    }

    @Override
    public boolean checkTableExistence(String tableName) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void createTable(String tableName, String indexDirectory, Schema schema, String luceneAnalyzer)
            throws TextDBException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteTable(String tableName) throws TextDBException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public IField insertTuple(String tableName, ITuple tuple) throws TextDBException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteTuple(String tableName, IField idValue) throws TextDBException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateTuple(String tableName, ITuple newTuple, IField idValue) throws TextDBException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ITuple getTuple(String tableName, IField idValue) throws TextDBException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IDataReader scanTable(String tableName) throws TextDBException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTableDirectory(String tableName) {
        DataStore collectionCatalogStore = new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY,
                CatalogConstants.COLLECTION_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), collectionCatalogStore,
                LuceneAnalyzerConstants.getStandardAnalyzer());
        scanPredicate.setIsPayloadAdded(false);
        DataReader collectionCatalogReader = new DataReader(scanPredicate);
        
        try {
            collectionCatalogReader.open();
            
            ITuple tuple;
            while ((tuple = collectionCatalogReader.getNextTuple()) != null) {
                IField nameField = tuple.getField(CatalogConstants.COLLECTION_NAME);
                if (nameField == null || !nameField.getValue().equals(tableName)) {
                    continue;
                }
                IField directoryField = tuple.getField(CatalogConstants.COLLECTION_DIRECTORY);
                if (directoryField != null) {
                    return directoryField.getValue().toString();
                } else {
                    return null;
                }
            }
            
            collectionCatalogReader.close();
        } catch (DataFlowException e) {
            return null;
        }
        return null;
    }

    @Override
    public Schema getTableSchema(String tableName) {
        String tableDirectory = getTableDirectory(tableName);
        if (tableDirectory == null) {
            return null;
        }
        
        return null;
    }

    @Override
    public String getTableAnalyzer(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }
    
    private static boolean catalogExists() {
        return DataReader.indexExists(CatalogConstants.COLLECTION_CATALOG_DIRECTORY)
                && DataReader.indexExists(CatalogConstants.SCHEMA_CATALOG_DIRECTORY);
    }
    

    /*
     * Write the initial collection catalog to catalog files.
     */
    private static void initializeCollectionCatalog() throws StorageException {
        DataStore collectionCatalogStore = new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY,
                CatalogConstants.COLLECTION_CATALOG_SCHEMA);
        DataWriter collectionCatalogWriter = new DataWriter(collectionCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer());
        collectionCatalogWriter.clearData();
        for (ITuple tuple : CatalogConstants.getInitialCollectionCatalogTuples()) {
            collectionCatalogWriter.insertTuple(tuple);
        }
    }

    /*
     * Write the initial schema catalog to catalog files.
     */
    private static void initializeSchemaCatalog() throws StorageException {
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        DataWriter schemaCatalogWriter = new DataWriter(schemaCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer());
        schemaCatalogWriter.clearData();
        for (ITuple tuple : CatalogConstants.getInitialSchemaCatalogTuples()) {
            schemaCatalogWriter.insertTuple(tuple);
        }
    }

}
