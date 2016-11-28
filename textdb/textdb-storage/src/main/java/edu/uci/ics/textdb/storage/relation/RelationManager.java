package edu.uci.ics.textdb.storage.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class RelationManager {
    
    private static volatile RelationManager singletonRelationManager = null;
    
    private RelationManager() throws StorageException {
        if (! checkCatalogExistence()) {
            initializeCatalog();
        }
    }

    public static RelationManager getRelationManager() throws StorageException {
        if (singletonRelationManager == null) {
            synchronized (RelationManager.class) {
                if (singletonRelationManager == null) {
                    singletonRelationManager = new RelationManager();
                }
            }
        }
        return singletonRelationManager;
    }

    public boolean checkTableExistence(String tableName) {
        try {
            getTableDirectory(tableName);
            return true;
        } catch (StorageException e) {
            return false;
        }
    }

    // create a new table, tableName must be unique
    public void createTable(String tableName, String indexDirectory, Schema schema, String luceneAnalyzer)
            throws StorageException {
        // table should not exist
        if (checkTableExistence(tableName)) {
            throw new StorageException(String.format("Table %s already exists.", tableName));
        }
        
        Schema tableSchema = Utils.getSchemaWithID(schema);
                
        // write table catalog
        DataStore tableCatalogStore = new DataStore(CatalogConstants.TABLE_CATALOG_DIRECTORY,
                CatalogConstants.TABLE_CATALOG_SCHEMA);      
        insertTupleToDirectory(tableCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer(),
                CatalogConstants.getTableCatalogTuple(tableName, indexDirectory, luceneAnalyzer));
       
        // write schema catalog
        DataStore schemaCatalogStore = new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY,
                CatalogConstants.SCHEMA_CATALOG_SCHEMA);
        for (ITuple tuple : CatalogConstants.getSchemaCatalogTuples(tableName, tableSchema)) {
            insertTupleToDirectory(schemaCatalogStore, LuceneAnalyzerConstants.getStandardAnalyzer(), tuple);
        }
        
    }

    // drop a table
    public void deleteTable(String tableName) throws StorageException {
        if (tableName.equals(CatalogConstants.TABLE_CATALOG) 
                || tableName.equals(CatalogConstants.SCHEMA_CATALOG)) {
            throw new StorageException("Deleting system catalog is prohibited.");
        }
        
        Query catalogTableNameQuery = new TermQuery(new Term(CatalogConstants.TABLE_NAME, tableName));
        
        DataWriter tableCatalogWriter = new DataWriter(CatalogConstants.TABLE_CATALOG_DATASTORE, 
                LuceneAnalyzerConstants.getStandardAnalyzer());
        tableCatalogWriter.deleteTuple(catalogTableNameQuery);
        
        DataWriter schemaCatalogWriter = new DataWriter(CatalogConstants.SCHEMA_CATALOG_DATASTORE,
                LuceneAnalyzerConstants.getStandardAnalyzer());
        schemaCatalogWriter.deleteTuple(catalogTableNameQuery);
    }

    // insert a tuple to a table, returns the ID field
    public IDField insertTuple(String tableName, ITuple tuple) throws StorageException {
        if (! checkTableExistence(tableName)) {
            throw new StorageException(String.format("Table %s doesn't exist.", tableName));
        }
        try {
            DataStore tableDataStore = getTableDataStore(tableName);
            String tableAnalyzerString = getTableAnalyzer(tableName);
            Analyzer luceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(tableAnalyzerString);

            return insertTupleToDirectory(tableDataStore, luceneAnalyzer, tuple);
        } catch (DataFlowException e) {
            throw new StorageException(e);
        }
    }
    
    private IDField insertTupleToDirectory(IDataStore dataStore, Analyzer luceneAnalyzer, ITuple tuple) throws StorageException {
        String tableDirectory = dataStore.getDataDirectory();
        Schema tableSchema = dataStore.getSchema();
        IDField idField;
                
        if (! tableSchema.containsField(SchemaConstants._ID)) {
            tableSchema = Utils.getSchemaWithID(tableSchema);
        }
        
        ITuple insertionTuple = tuple;
        // add "_id" to schema, and add ID field to tuple
        if (! insertionTuple.getSchema().containsField(SchemaConstants._ID)) {
            idField = new IDField(UUID.randomUUID().toString());
            insertionTuple = getTupleWithID(tuple, idField);
        } else {
            idField = (IDField) tuple.getField(SchemaConstants._ID);
        }

        if (! tableSchema.equals(insertionTuple.getSchema())) {
            throw new StorageException("Tuple's schema is inconsistent with table schema.");
        }
        
        DataWriter dataWriter = new DataWriter(new DataStore(tableDirectory, tableSchema), luceneAnalyzer);
        dataWriter.insertTuple(insertionTuple);

        return idField;
    }

    // delete a tuple by its id
    public void deleteTuple(String tableName, IField idValue) throws StorageException {
        try {
            DataStore tableDataStore = getTableDataStore(tableName);
            String tableAnalyzerString = getTableAnalyzer(tableName);
            Analyzer luceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(tableAnalyzerString);
            
            Query tupleIDQuery = new TermQuery(new Term(SchemaConstants._ID, idValue.getValue().toString()));
            DataWriter tableDataWriter = new DataWriter(tableDataStore, luceneAnalyzer);
            tableDataWriter.deleteTuple(tupleIDQuery);   
        } catch (DataFlowException e) {
            throw new StorageException(e);
        }   
    }
    
    public void deleteTuple(String tableName, Query...deletionQueries) throws StorageException {
        
    }
    
    

    // update a tuple by its id
    public void updateTuple(String tableName, ITuple newTuple, IDField idValue) throws StorageException {
        if (getTuple(tableName, idValue) == null) {
            throw new StorageException(
                    String.format("Tuple with id %s doesn't exist in table %s.", idValue, tableName));
        }

        ITuple newTupleWithID = getTupleWithID(newTuple, (IDField) idValue);
        if (newTupleWithID.getField(SchemaConstants._ID) != (IDField) idValue) {
            throw new StorageException("New tuple's ID is inconsistent with idValue.");
        }
        
        deleteTuple(tableName, idValue);
        insertTuple(tableName, newTupleWithID);
    }

    // get a tuple by its id
    public ITuple getTuple(String tableName, IDField idValue) throws StorageException {
        DataStore tableDataStore = getTableDataStore(tableName);
        
        Query tupleIDQuery = new TermQuery(new Term(SchemaConstants._ID, idValue.getValue().toString()));
        
        DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(tupleIDQuery, tableDataStore);
        dataReaderPredicate.setIsPayloadAdded(false);
        DataReader dataReader = new DataReader(dataReaderPredicate);
        
        dataReader.open(); 
        ITuple tuple = dataReader.getNextTuple();
        dataReader.close();

        return tuple;
    }

    // get the dataReader to scan a table
    public IDataReader scanTable(String tableName) throws StorageException {
        DataStore tableDataStore = getTableDataStore(tableName);
        DataReader dataReader = new DataReader(DataReaderPredicate.getScanPredicate(tableDataStore));
        return dataReader;
    }
    
    // get the table's DataStore
    public DataStore getTableDataStore(String tableName) throws StorageException {
        String tableDirectory = getTableDirectory(tableName);
        Schema tableSchema = getTableSchema(tableName);
        return new DataStore(tableDirectory, tableSchema);
    }

    // get the table's Directory
    public String getTableDirectory(String tableName) throws StorageException {
        DataStore tableCatalogStore = new DataStore(CatalogConstants.TABLE_CATALOG_DIRECTORY,
                CatalogConstants.TABLE_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), tableCatalogStore);
        scanPredicate.setIsPayloadAdded(false);
        DataReader tableCatalogReader = new DataReader(scanPredicate);

        tableCatalogReader.open();
        ITuple nextTuple;
        while ((nextTuple = tableCatalogReader.getNextTuple()) != null) {
            IField tableNameField = nextTuple.getField(CatalogConstants.TABLE_NAME);
            // field must not be null
            if (tableNameField == null) {
                continue;
            }
            // table name must be the same (case insensitive)
            String tableNameString = tableNameField.getValue().toString();
            if (! tableNameString.toLowerCase().equals(tableName.toLowerCase())) {
                continue;
            }
            IField directoryField = nextTuple.getField(CatalogConstants.TABLE_DIRECTORY);
            if (directoryField == null) {
                break;
            }
            return directoryField.getValue().toString();
        }
        tableCatalogReader.close();

        throw new StorageException(String.format("The directory for table %s is not found.", tableName));
    }

    // get the table's schema
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
            IField tableNameField = nextTuple.getField(CatalogConstants.TABLE_NAME);
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
        List<Attribute> tableSchemaData = tableAttributeTuples.stream()
                // sort the tuples based on the attributePosition field.
                .sorted((tuple1, tuple2) -> Integer.compare((int) tuple1.getField(CatalogConstants.ATTR_POSITION).getValue(), 
                        (int) tuple2.getField(CatalogConstants.ATTR_POSITION).getValue()))
                // map one tuple to one attribute
                .map(tuple -> new Attribute(tuple.getField(CatalogConstants.ATTR_NAME).getValue().toString(),
                        convertAttributeType(tuple.getField(CatalogConstants.ATTR_TYPE).getValue().toString())))
                .collect(Collectors.toList());
        
        return new Schema(tableSchemaData.stream().toArray(Attribute[]::new));
    }

    // get the table's lucene analyzer
    public String getTableAnalyzer(String tableName) throws StorageException {
        DataStore tableCatalogStore = new DataStore(CatalogConstants.TABLE_CATALOG_DIRECTORY,
                CatalogConstants.TABLE_CATALOG_SCHEMA);
        DataReaderPredicate scanPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), tableCatalogStore);
        scanPredicate.setIsPayloadAdded(false);
        DataReader tableCatalogReader = new DataReader(scanPredicate);

        tableCatalogReader.open();
        ITuple nextTuple;
        while ((nextTuple = tableCatalogReader.getNextTuple()) != null) {
            IField tableNameField = nextTuple.getField(CatalogConstants.TABLE_NAME);
            // field must not be null
            if (tableNameField == null) {
                continue;
            }
            // table name must be the same (case insensitive)
            String tableNameString = tableNameField.getValue().toString();
            if (! tableNameString.toLowerCase().equals(tableName.toLowerCase())) {
                continue;
            }
            IField analyzerField = nextTuple.getField(CatalogConstants.TABLE_LUCENE_ANALYZER);
            if (analyzerField == null) {
                break;
            }
            return analyzerField.getValue().toString();
        }
        tableCatalogReader.close();

        throw new StorageException(String.format("The analyzer for table %s is not found.", tableName));
    }
    
    private static boolean checkCatalogExistence() {
        return DataReader.checkIndexExistence(CatalogConstants.TABLE_CATALOG_DIRECTORY)
                && DataReader.checkIndexExistence(CatalogConstants.SCHEMA_CATALOG_DIRECTORY);
    }
    
    private void initializeCatalog() throws StorageException {
        createTable(CatalogConstants.TABLE_CATALOG, 
                CatalogConstants.TABLE_CATALOG_DIRECTORY, 
                CatalogConstants.TABLE_CATALOG_SCHEMA,
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
    
    private static ITuple getTupleWithID(ITuple tuple, IDField _id) {
        ITuple tupleWithID = tuple;
        
        Schema tupleSchema = tuple.getSchema();
        if (! tupleSchema.containsField(SchemaConstants._ID)) {
            tupleSchema = Utils.getSchemaWithID(tupleSchema);
            List<IField> newTupleFields = new ArrayList<>();
            newTupleFields.add(_id);
            newTupleFields.addAll(tuple.getFields());
            tupleWithID = new DataTuple(tupleSchema, newTupleFields.stream().toArray(IField[]::new));
        }
        
        return tupleWithID;
    }

}
