package edu.uci.ics.textdb.storage.relation;

import java.io.File;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.utils.Utils;

public class RelationTest {
    
    RelationManager relationManager;
    
    @Before
    public void setUpRelationManager() throws TextDBException {
        relationManager = RelationManager.getRelationManager();
    }
    
    /*
     * Test the information about "collection catalog" itself is stored properly.
     * 
     */
    @Test
    public void test1() throws Exception {
        String collectionCatalogDirectory = 
                relationManager.getTableDirectory(CatalogConstants.COLLECTION_CATALOG);
        String collectionCatalogLuceneAnalyzer = 
                relationManager.getTableAnalyzer(CatalogConstants.COLLECTION_CATALOG);
        Schema collectionCatalogSchema = 
                relationManager.getTableSchema(CatalogConstants.COLLECTION_CATALOG);
                
        Assert.assertEquals(collectionCatalogDirectory, 
                new File(CatalogConstants.COLLECTION_CATALOG_DIRECTORY).getCanonicalPath());
        Assert.assertEquals(collectionCatalogLuceneAnalyzer, LuceneAnalyzerConstants.standardAnalyzerString());
        Assert.assertEquals(collectionCatalogSchema, Utils.getSchemaWithID(CatalogConstants.COLLECTION_CATALOG_SCHEMA));
    }
    
    /*
     * Test the information about "schema catalog" itself is stored properly.
     */
    @Test
    public void test2() throws Exception {
        String schemaCatalogDirectory = 
                relationManager.getTableDirectory(CatalogConstants.SCHEMA_CATALOG);
        String schemaCatalogLuceneAnalyzer = 
                relationManager.getTableAnalyzer(CatalogConstants.SCHEMA_CATALOG);
        Schema schemaCatalogSchema = 
                relationManager.getTableSchema(CatalogConstants.SCHEMA_CATALOG);
        
        Assert.assertEquals(schemaCatalogDirectory, 
                new File(CatalogConstants.SCHEMA_CATALOG_DIRECTORY).getCanonicalPath());
        Assert.assertEquals(schemaCatalogLuceneAnalyzer, LuceneAnalyzerConstants.standardAnalyzerString());
        Assert.assertEquals(schemaCatalogSchema, Utils.getSchemaWithID(CatalogConstants.SCHEMA_CATALOG_SCHEMA));  
    }
    
    /*
     * Create a table and test if table's information can be retrieved successfully.
     */
    @Test
    public void test3() throws Exception {        
        String collectionName = "relation_manager_test_collection_1";
        String collectionDirectory = "./index/test_collection_1/";
        Schema collectionSchema = new Schema(
                new Attribute("id", FieldType.INTEGER), new Attribute("city", FieldType.STRING),
                new Attribute("description", FieldType.TEXT), new Attribute("tax rate", FieldType.DOUBLE),
                new Attribute("population", FieldType.INTEGER), new Attribute("record time", FieldType.DATE));
        String collectionLuceneAnalyzer = LuceneAnalyzerConstants.standardAnalyzerString();
        
        RelationManager relationManager = RelationManager.getRelationManager();
        
        relationManager.deleteTable(collectionName);
        
        relationManager.createTable(
                collectionName, collectionDirectory, collectionSchema, collectionLuceneAnalyzer);
        
        Assert.assertEquals(new File(collectionDirectory).getCanonicalPath(), 
                relationManager.getTableDirectory(collectionName));
        Assert.assertEquals(Utils.getSchemaWithID(collectionSchema), relationManager.getTableSchema(collectionName));
        Assert.assertEquals(collectionLuceneAnalyzer, relationManager.getTableAnalyzer(collectionName));
        
        relationManager.deleteTable(collectionName);
    }
    
    /*
     * Retrieving the directory of a collection which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test4() throws Exception {
        String collectionName = "relation_manager_test_collection_1";
        RelationManager.getRelationManager().getTableDirectory(collectionName);
    }
    
    /*
     * Retrieving the schema of a collection which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test5() throws Exception {
        String collectionName = "relation_manager_test_collection_1";
        RelationManager.getRelationManager().getTableSchema(collectionName);
    }
    
    /*
     * Retrieving the lucene analyzer of a collection which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test6() throws Exception {
        String collectionName = "relation_manager_test_collection_1";
        RelationManager.getRelationManager().getTableAnalyzer(collectionName);
    }
    

    /*
     * Test creating and deleting multiple tables in relation manager.
     */
    @Test
    public void test7() throws Exception {
        String collectionName = "relation_manager_test_collection";
        String collectionDirectory = "./index/test_collection";
        Schema collectionSchema = new Schema(
                new Attribute("id", FieldType.INTEGER), new Attribute("city", FieldType.STRING),
                new Attribute("description", FieldType.TEXT), new Attribute("tax rate", FieldType.DOUBLE),
                new Attribute("population", FieldType.INTEGER), new Attribute("record time", FieldType.DATE));
        
        int NUM_OF_LOOPS = 20;
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create tables
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            // delete previously inserted tables first
            relationManager.deleteTable(
                    collectionName + '_' + i);
            relationManager.createTable(
                    collectionName + '_' + i,
                    collectionDirectory + '_' + i, 
                    collectionSchema, 
                    LuceneAnalyzerConstants.standardAnalyzerString());
        }
        // assert tables are correctly created
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            Assert.assertEquals(new File(collectionDirectory + '_' + i).getCanonicalPath(), 
                    relationManager.getTableDirectory(collectionName + '_' + i));
            Assert.assertEquals(Utils.getSchemaWithID(collectionSchema), relationManager.getTableSchema(collectionName + '_' + i));
            Assert.assertEquals(LuceneAnalyzerConstants.standardAnalyzerString(), 
                    relationManager.getTableAnalyzer(collectionName + '_' + i));
        }
        // delete collections
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            relationManager.deleteTable(collectionName + '_' + i);
        }
        // assert collections are correctly deleted
        int errorCount = 0;
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            try {
                relationManager.getTableDirectory(collectionName + '_' + i);
            } catch (StorageException e) {
                errorCount++;
            }
        }
        Assert.assertEquals(NUM_OF_LOOPS, errorCount);
    }
    
    
}
