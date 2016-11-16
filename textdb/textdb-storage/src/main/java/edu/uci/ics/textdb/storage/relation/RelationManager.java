package edu.uci.ics.textdb.storage.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IRelationManager;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class RelationManager implements IRelationManager {
    
    private static volatile RelationManager singletonRelationManager = null;
    
    private RelationManager() throws TextDBException {
        if (! checkCatalogExistence()) {
            initializeCatalog();
        }
    }

    public static RelationManager getRelationManager() throws TextDBException {
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
        try {
            String tableDirectory = getTableDirectory(tableName);
            return DataReader.checkIndexExistence(tableDirectory);
        } catch (StorageException e) {
            return false;
        }
    }

    @Override
    public void createTable(String tableName, String indexDirectory, Schema schema, String luceneAnalyzer)
            throws TextDBException {
        // table should not exist
        if (checkTableExistence(tableName)) {
            throw new StorageException(String.format("Table %s already exists.", tableName));
        }
        
        Schema tableSchema = Utils.getSchemaWithID(schema);
                
        // write collection catalog
        DataStore collectionCatalogStore = new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY,
                CatalogConstants.COLLECTION_CATALOG_SCHEMA);      
        insertTupleToDirectory(collectionCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer(),
                CatalogConstants.getCollectionCatalogTuple(tableName, indexDirectory, luceneAnalyzer));
       
        // write schema catalog
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        for (ITuple tuple : CatalogConstants.getSchemaCatalogTuples(tableName, tableSchema)) {
            insertTupleToDirectory(schemaCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer(), tuple);
        }
        
    }

    @Override
    public void deleteTable(String tableName) throws TextDBException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public IDField insertTuple(String tableName, ITuple tuple) throws TextDBException {
        if (! checkTableExistence(tableName)) {
            throw new StorageException(String.format("Table %s doesn't exist.", tableName));
        }
        
        String tableDirectory = getTableDirectory(tableName);
        Schema tableSchema = getTableSchema(tableName);
        String tableAnalyzerString = getTableAnalyzer(tableName);
        Analyzer luceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(tableAnalyzerString);

        return insertTupleToDirectory(new DataStore(tableDirectory, tableSchema), luceneAnalyzer, tuple);
    }
    
    private IDField insertTupleToDirectory(IDataStore dataStore, Analyzer luceneAnalyzer, ITuple tuple) throws TextDBException {
        String tableDirectory = dataStore.getDataDirectory();
        Schema tableSchema = dataStore.getSchema();
        IDField idField;
                
        if (! tableSchema.containsField(SchemaConstants._ID)) {
            tableSchema = Utils.getSchemaWithID(tableSchema);
        }
        
        Schema insertionSchema = tuple.getSchema();
        ITuple insertionTuple = tuple;
        // add "_id" to schema, and add ID field to tuple
        if (! insertionSchema.containsField(SchemaConstants._ID)) {
            insertionSchema = Utils.getSchemaWithID(insertionSchema);
            idField = new IDField(UUID.randomUUID().toString());
            List<IField> newTupleFields = new ArrayList<>();
            newTupleFields.add(idField);
            newTupleFields.addAll(tuple.getFields());
            insertionTuple = new DataTuple(insertionSchema, newTupleFields.stream().toArray(IField[]::new));
        } else {
            idField = (IDField) tuple.getField(SchemaConstants._ID);
        }
        
        if (! tableSchema.equals(insertionSchema)) {
            throw new StorageException("Tuple's schema is inconsistent with table schema.");
        }
        
        DataWriter dataWriter = new DataWriter(new DataStore(tableDirectory, tableSchema), luceneAnalyzer);
        dataWriter.insertTuple(insertionTuple);

        return idField;    
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
    public String getTableDirectory(String tableName) throws StorageException {
        DataStore collectionCatalogStore = new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY,
                CatalogConstants.COLLECTION_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), collectionCatalogStore);
        scanPredicate.setIsPayloadAdded(false);
        DataReader collectionCatalogReader = new DataReader(scanPredicate);

        collectionCatalogReader.open();
        ITuple nextTuple;
        while ((nextTuple = collectionCatalogReader.getNextTuple()) != null) {
            IField tableNameField = nextTuple.getField(CatalogConstants.COLLECTION_NAME);
            // field must not be null
            if (tableNameField == null) {
                continue;
            }
            // table name must be the same (case insensitive)
            String tableNameString = tableNameField.getValue().toString();
            if (! tableNameString.toLowerCase().equals(tableName.toLowerCase())) {
                continue;
            }
            IField directoryField = nextTuple.getField(CatalogConstants.COLLECTION_DIRECTORY);
            if (directoryField == null) {
                break;
            }
            return directoryField.getValue().toString();
        }
        collectionCatalogReader.close();

        throw new StorageException(String.format("The directory for table %s is not found.", tableName));
    }

    @Override
    public Schema getTableSchema(String tableName) throws StorageException {
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), schemaCatalogStore);
        scanPredicate.setIsPayloadAdded(false);
        DataReader schemaCatalogReader = new DataReader(scanPredicate);   
        
        // get all the tuples of this table's attributes
        schemaCatalogReader.open();
        List<ITuple> tableAttributeTuples = new ArrayList<>();
        ITuple nextTuple;
        
        while ((nextTuple = schemaCatalogReader.getNextTuple()) != null) {
            IField tableNameField = nextTuple.getField(CatalogConstants.COLLECTION_NAME);
            // field must not be null
            if (tableNameField == null) {
                continue;
            }
            // table name must be the same (case insensitive)
            String tableNameString = tableNameField.getValue().toString();
            if (! tableNameString.toLowerCase().equals(tableName.toLowerCase())) {
                continue;
            }
            tableAttributeTuples.add(nextTuple);
        }
        schemaCatalogReader.close();
        
        // Schema is not found if the list is empty.
        if (tableAttributeTuples.isEmpty()) {
            throw new StorageException(String.format("The schema of table %s is not found.", tableName));
        }
        
        // convert the unordered list of tuples to an order list of attributes
        List<Attribute> collectionSchemaData = tableAttributeTuples.stream()
                // sort the tuples based on the attributePosition field.
                .sorted((tuple1, tuple2) -> Integer.compare((int) tuple1.getField(CatalogConstants.ATTR_POSITION).getValue(), 
                        (int) tuple2.getField(CatalogConstants.ATTR_POSITION).getValue()))
                // map one tuple to one attribute
                .map(tuple -> new Attribute(tuple.getField(CatalogConstants.ATTR_NAME).getValue().toString(),
                        convertAttributeType(tuple.getField(CatalogConstants.ATTR_TYPE).getValue().toString())))
                .collect(Collectors.toList());
        
        return new Schema(collectionSchemaData.stream().toArray(Attribute[]::new));
    }

    @Override
    public String getTableAnalyzer(String tableName) throws StorageException {
        DataStore collectionCatalogStore = new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY,
                CatalogConstants.COLLECTION_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), collectionCatalogStore);
        scanPredicate.setIsPayloadAdded(false);
        DataReader collectionCatalogReader = new DataReader(scanPredicate);

        collectionCatalogReader.open();
        ITuple nextTuple;
        while ((nextTuple = collectionCatalogReader.getNextTuple()) != null) {
            IField tableNameField = nextTuple.getField(CatalogConstants.COLLECTION_NAME);
            // field must not be null
            if (tableNameField == null) {
                continue;
            }
            // table name must be the same (case insensitive)
            String tableNameString = tableNameField.getValue().toString();
            if (! tableNameString.toLowerCase().equals(tableName.toLowerCase())) {
                continue;
            }
            IField analyzerField = nextTuple.getField(CatalogConstants.COLLECTION_LUCENE_ANALYZER);
            if (analyzerField == null) {
                break;
            }
            return analyzerField.getValue().toString();
        }
        collectionCatalogReader.close();

        throw new StorageException(String.format("The analyzer for table %s is not found.", tableName));
    }
    
    private static boolean checkCatalogExistence() {
        return DataReader.checkIndexExistence(CatalogConstants.COLLECTION_CATALOG_DIRECTORY)
                && DataReader.checkIndexExistence(CatalogConstants.SCHEMA_CATALOG_DIRECTORY);
    }
    
    private void initializeCatalog() throws TextDBException {
        createTable(CatalogConstants.COLLECTION_CATALOG, 
                CatalogConstants.COLLECTION_CATALOG_DIRECTORY, 
                CatalogConstants.COLLECTION_CATALOG_SCHEMA,
                LuceneAnalyzerConstants.standardAnalyzerString());
        createTable(CatalogConstants.SCHEMA_CATALOG,
                CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA,
                LuceneAnalyzerConstants.standardAnalyzerString());
    }
    
    
    /**
     * This function converts a attributeTypeString to FieldType (case insensitive). 
     * It returns null if string is not a valid type.
     * 
     * @param attributeTypeStr
     * @return FieldType, null if attributeTypeStr is not a valid type.
     */
    private static FieldType convertAttributeType(String attributeTypeStr) {
        return Stream.of(FieldType.values())
                .filter(typeStr -> typeStr.toString().toLowerCase().equals(attributeTypeStr.toLowerCase()))
                .findAny().orElse(null);
    }    

}
