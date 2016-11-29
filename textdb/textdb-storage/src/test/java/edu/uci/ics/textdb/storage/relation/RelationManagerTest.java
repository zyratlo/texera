package edu.uci.ics.textdb.storage.relation;

import java.io.File;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.utils.Utils;

public class RelationManagerTest {
    
    RelationManager relationManager;
    
    @Before
    public void setUpRelationManager() throws TextDBException {
        relationManager = RelationManager.getRelationManager();
    }
    
    /*
     * Test the information about "table catalog" itself is stored properly.
     * 
     */
    @Test
    public void test1() throws Exception {
        String tableCatalogDirectory = 
                relationManager.getTableDirectory(CatalogConstants.TABLE_CATALOG);
        Analyzer tableCatalogLuceneAnalyzer = 
                relationManager.getTableAnalyzer(CatalogConstants.TABLE_CATALOG);
        Schema tableCatalogSchema = 
                relationManager.getTableSchema(CatalogConstants.TABLE_CATALOG);
                
        Assert.assertEquals(tableCatalogDirectory, 
                new File(CatalogConstants.TABLE_CATALOG_DIRECTORY).getCanonicalPath());
        Assert.assertTrue(tableCatalogLuceneAnalyzer instanceof StandardAnalyzer);
        Assert.assertEquals(tableCatalogSchema, Utils.getSchemaWithID(CatalogConstants.TABLE_CATALOG_SCHEMA));
    }
    
    /*
     * Test the information about "schema catalog" itself is stored properly.
     */
    @Test
    public void test2() throws Exception {
        String schemaCatalogDirectory = 
                relationManager.getTableDirectory(CatalogConstants.SCHEMA_CATALOG);
        Analyzer schemaCatalogLuceneAnalyzer = 
                relationManager.getTableAnalyzer(CatalogConstants.SCHEMA_CATALOG);
        Schema schemaCatalogSchema = 
                relationManager.getTableSchema(CatalogConstants.SCHEMA_CATALOG);
        
        Assert.assertEquals(schemaCatalogDirectory, 
                new File(CatalogConstants.SCHEMA_CATALOG_DIRECTORY).getCanonicalPath());
        Assert.assertTrue(schemaCatalogLuceneAnalyzer instanceof StandardAnalyzer);
        Assert.assertEquals(schemaCatalogSchema, Utils.getSchemaWithID(CatalogConstants.SCHEMA_CATALOG_SCHEMA));  
    }
    
    /*
     * Create a table and test if table's information can be retrieved successfully.
     */
    @Test
    public void test3() throws Exception {        
        String tableName = "relation_manager_test_table_1";
        String tableDirectory = "./index/test_table_1/";
        Schema tableSchema = new Schema(
                new Attribute("city", FieldType.STRING),
                new Attribute("description", FieldType.TEXT), new Attribute("tax rate", FieldType.DOUBLE),
                new Attribute("population", FieldType.INTEGER), new Attribute("record time", FieldType.DATE));
        String tableLuceneAnalyzerString = LuceneAnalyzerConstants.standardAnalyzerString();
        Analyzer tableLuceneAnalyzer = LuceneAnalyzerConstants.getLuceneAnalyzer(tableLuceneAnalyzerString);
        
        RelationManager relationManager = RelationManager.getRelationManager();
        
        relationManager.deleteTable(tableName);
        
        relationManager.createTable(
                tableName, tableDirectory, tableSchema, tableLuceneAnalyzerString);
        
        Assert.assertEquals(new File(tableDirectory).getCanonicalPath(), 
                relationManager.getTableDirectory(tableName));
        Assert.assertEquals(Utils.getSchemaWithID(tableSchema), relationManager.getTableSchema(tableName));
        Assert.assertEquals(tableLuceneAnalyzer.getClass(), relationManager.getTableAnalyzer(tableName).getClass());
        
        relationManager.deleteTable(tableName);
    }
    
    /*
     * Retrieving the directory of a table which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test4() throws Exception {
        String tableName = "relation_manager_test_table_1";
        RelationManager.getRelationManager().getTableDirectory(tableName);
    }
    
    /*
     * Retrieving the schema of a table which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test5() throws Exception {
        String tableName = "relation_manager_test_table_1";
        RelationManager.getRelationManager().getTableSchema(tableName);
    }
    
    /*
     * Retrieving the lucene analyzer of a table which doesn't exist should result in an exception.
     */
    @Test(expected = StorageException.class)
    public void test6() throws Exception {
        String tableName = "relation_manager_test_table_1";
        RelationManager.getRelationManager().getTableAnalyzer(tableName);
    }
    

    /*
     * Test creating and deleting multiple tables in relation manager.
     */
    @Test
    public void test7() throws Exception {
        String tableName = "relation_manager_test_table";
        String tableDirectory = "./index/test_table";
        Schema tableSchema = new Schema(
                new Attribute("city", FieldType.STRING),
                new Attribute("description", FieldType.TEXT), new Attribute("tax rate", FieldType.DOUBLE),
                new Attribute("population", FieldType.INTEGER), new Attribute("record time", FieldType.DATE));
        
        int NUM_OF_LOOPS = 10;
        RelationManager relationManager = RelationManager.getRelationManager();
        
        // create tables
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            // delete previously inserted tables first
            relationManager.deleteTable(
                    tableName + '_' + i);
            relationManager.createTable(
                    tableName + '_' + i,
                    tableDirectory + '_' + i, 
                    tableSchema, 
                    LuceneAnalyzerConstants.standardAnalyzerString());
        }
        // assert tables are correctly created
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            Assert.assertEquals(new File(tableDirectory + '_' + i).getCanonicalPath(), 
                    relationManager.getTableDirectory(tableName + '_' + i));
            Assert.assertEquals(Utils.getSchemaWithID(tableSchema), relationManager.getTableSchema(tableName + '_' + i));
        }
        // delete tables
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            relationManager.deleteTable(tableName + '_' + i);
        }
        // assert tables are correctly deleted
        int errorCount = 0;
        for (int i = 0; i < NUM_OF_LOOPS; i++) {
            try {
                relationManager.getTableDirectory(tableName + '_' + i);
            } catch (StorageException e) {
                errorCount++;
            }
        }
        Assert.assertEquals(NUM_OF_LOOPS, errorCount);
    }
    
    /*
     * Test inserting a tuple to a table and then delete it 
     */
    @Test
    public void test8() throws Exception {
        String tableName = "relation_manager_test_table";
        String tableDirectory = "./index/test_table";
        Schema tableSchema = new Schema(
                new Attribute("content", FieldType.STRING));
        
        RelationManager relationManager = RelationManager.getRelationManager();
        
        relationManager.deleteTable(tableName);
        relationManager.createTable(
                tableName, tableDirectory, tableSchema, LuceneAnalyzerConstants.standardAnalyzerString());
        
        ITuple insertedTuple = new DataTuple(tableSchema, new StringField("test"));
        IDField idField = relationManager.insertTuple(tableName, insertedTuple);
        
        ITuple returnedTuple = relationManager.getTuple(tableName, idField);
        
        Assert.assertEquals(insertedTuple.getField("content").getValue().toString(), 
                returnedTuple.getField("content").getValue().toString());
        
        relationManager.deleteTuple(tableName, idField);
        
        ITuple deletedTuple = relationManager.getTuple(tableName, idField);
        // should not reach next line because of exception
        Assert.assertNull(deletedTuple);
        
        relationManager.deleteTable(tableName);       
    }
    
    /*
     * Test inserting a tuple to a table, then update it, then delete it 
     */
    @Test
    public void test9() throws Exception {
        String tableName = "relation_manager_test_table";
        String tableDirectory = "./index/test_table";
        Schema tableSchema = new Schema(
                new Attribute("content", FieldType.STRING));
        
        RelationManager relationManager = RelationManager.getRelationManager();
        
        relationManager.deleteTable(tableName);
        relationManager.createTable(
                tableName, tableDirectory, tableSchema, LuceneAnalyzerConstants.standardAnalyzerString());
        
        ITuple insertedTuple = new DataTuple(tableSchema, new StringField("test"));
        IDField idField = relationManager.insertTuple(tableName, insertedTuple);   
        ITuple returnedTuple = relationManager.getTuple(tableName, idField);
        
        Assert.assertEquals(insertedTuple.getField("content").getValue().toString(), 
                returnedTuple.getField("content").getValue().toString());
        
        
        ITuple updatedTuple = new DataTuple(tableSchema, new StringField("testUpdate"));
        relationManager.updateTuple(tableName, updatedTuple, idField);
        ITuple returnedUpdatedTuple = relationManager.getTuple(tableName, idField);
        
        Assert.assertEquals(updatedTuple.getField("content").getValue().toString(), 
                returnedUpdatedTuple.getField("content").getValue().toString());
        
        relationManager.deleteTuple(tableName, idField);
        
        ITuple deletedTuple = relationManager.getTuple(tableName, idField);
        // should not reach next line because of exception
        Assert.assertNull(deletedTuple);
        
        relationManager.deleteTable(tableName);       
    }
    
    
}
