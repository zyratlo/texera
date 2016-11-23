package edu.uci.ics.textdb.storage.relation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * CatalogConstants stores the schema and the initial tuples of the catalog manager
 * 
 * 
 * ============================================
 * 
 * Initial tuples for the collection catalog:
 * 
 *  collectionName    |    collectionDirectory    |    luceneAnalyzer
 * 
 * collectionCatalog    ../catalog/collection       standardLuceneAnalyzer
 *   schemaCatalog      ../catalog/schema           standardLuceneAnalyzer
 *   
 * ============================================
 *   
 *  Initial tuples for the schema catalog. 
 *    collectionName    |    attributeName    |  attributeType  |  attributePosition
 *  
 *   collectionCatalog       collectionName         string                0
 *   collectionCatalog    collectionDirectory       string                1
 *   collectionCatalog       luceneAnalyzer         string                2
 *     schemaCatalog          collectionName        string                0
 *     schemaCatalog           attributeName        string                1
 *     schemaCatalog           attributeType        string                2
 *     schemaCatalog         attributePosition      string                3
 *     
 * ============================================
 * 
 * @author Zuozhi Wang
 *
 */
public class CatalogConstants {

    public static final String COLLECTION_CATALOG = "collectionCatalog";
    public static final String SCHEMA_CATALOG = "schemaCatalog";

    public static final String COLLECTION_CATALOG_DIRECTORY = "../catalog/collection";
    public static final String SCHEMA_CATALOG_DIRECTORY = "../catalog/schema";
    
    public static final DataStore COLLECTION_CATALOG_DATASTORE = 
            new DataStore(CatalogConstants.COLLECTION_CATALOG_DIRECTORY, CatalogConstants.COLLECTION_CATALOG_SCHEMA);
    public static final DataStore SCHEMA_CATALOG_DATASTORE = 
            new DataStore(CatalogConstants.SCHEMA_CATALOG_DIRECTORY, CatalogConstants.SCHEMA_CATALOG_SCHEMA);

    // Schema for the "collection catalog" table
    public static final String COLLECTION_NAME = "collectionName";
    public static final String COLLECTION_DIRECTORY = "collectionDirectory";
    public static final String COLLECTION_LUCENE_ANALYZER = "luceneAnalyzer";

    public static final Attribute COLLECTION_NAME_ATTR = new Attribute(COLLECTION_NAME, FieldType.STRING);
    public static final Attribute COLLECTION_DIRECTORY_ATTR = new Attribute(COLLECTION_DIRECTORY, FieldType.STRING);
    public static final Attribute COLLECTION_LUCENE_ANALYZER_ATTR = new Attribute(COLLECTION_LUCENE_ANALYZER,
            FieldType.STRING);

    public static final Schema COLLECTION_CATALOG_SCHEMA = new Schema(COLLECTION_NAME_ATTR, COLLECTION_DIRECTORY_ATTR,
            COLLECTION_LUCENE_ANALYZER_ATTR);

    // Schema for "schema catalog" table
    public static final String ATTR_NAME = "attributeName";
    public static final String ATTR_TYPE = "attributeType";
    public static final String ATTR_POSITION = "attributePosition";

    public static final Attribute ATTR_NAME_ATTR = new Attribute(ATTR_NAME, FieldType.STRING);
    public static final Attribute ATTR_TYPE_ATTR = new Attribute(ATTR_TYPE, FieldType.STRING);
    public static final Attribute ATTR_POSITION_ATTR = new Attribute(ATTR_POSITION, FieldType.INTEGER);

    public static final Schema SCHEMA_CATALOG_SCHEMA = new Schema(COLLECTION_NAME_ATTR, ATTR_NAME_ATTR, ATTR_TYPE_ATTR,
            ATTR_POSITION_ATTR);


    public static ITuple getCollectionCatalogTuple(String tableName, String tableDirectory, String luceneAnalyzerStr) 
            throws StorageException {
        try {
            String tableDirectoryAbsolute = new File(tableDirectory).getCanonicalPath();
            return new DataTuple(COLLECTION_CATALOG_SCHEMA, 
                    new StringField(tableName), 
                    new StringField(tableDirectoryAbsolute),
                    new StringField(luceneAnalyzerStr));
        } catch (IOException e) {
            throw new StorageException(String.format("Error occurs when getting the canonical path of %s.", tableDirectory));
        }

    }
    
    public static List<ITuple> getSchemaCatalogTuples(String tableName, Schema tableSchema) {
        List<ITuple> schemaCatalogTuples = new ArrayList<>();
        for (int i = 0; i < tableSchema.getAttributes().size(); i++) {
            Attribute attr = tableSchema.getAttributes().get(i);
            ITuple schemaTuple = new DataTuple(SCHEMA_CATALOG_SCHEMA, 
                    new StringField(tableName),
                    new StringField(attr.getFieldName()),
                    new StringField(attr.getFieldType().toString().toLowerCase()),
                    new IntegerField(i));
            schemaCatalogTuples.add(schemaTuple);
        }
        return schemaCatalogTuples;
    }

}
