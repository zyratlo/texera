package edu.uci.ics.textdb.exp.source.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import junit.framework.Assert;

public class FileSourceOperatorTest {
    
    public static Path tempFolderPath = Paths.get("./index/test_tables/filesource/tempfolder/");
    
    public static Path tempFile1Path = tempFolderPath.resolve("test1.txt");
    public static String tempFile1String = "File Source Operator Test File 1. This file should be included.";
    
    public static Path tempFile2Path = tempFolderPath.resolve("test2.txt");
    public static String tempFile2String = "File Source Operator Test File 2. This file should be included.";
    
    public static Path tempFile3Path = tempFolderPath.resolve("test3.tmp");
    public static String tempFile3String = "File Source Operator Test File 3. This file should NOT be included. "
            + "Because its extension (.tmp) is not supported.";

    public static Path nestedFolderPath = tempFolderPath.resolve("nested/");
    
    public static Path tempFile4Path = nestedFolderPath.resolve("test4.txt");
    public static String tempFile4String = "File Source Operator Test File 3. The This should be in the files. "
            + "Because it is in a nested sub-folder.";
    
    // a helper Files.delete function that throws a RuntimException
    private static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
   
    @BeforeClass
    public static void setup() throws IOException {
        // make sure the files are deleted
        cleanUp();
                
        // create a temp folder
        Files.createDirectories(tempFolderPath);
        // create multiple files under this directory
        Files.createFile(tempFile1Path);
        Files.write(tempFile1Path, tempFile1String.getBytes());
        
        Files.createFile(tempFile2Path);
        Files.write(tempFile2Path, tempFile2String.getBytes());
        
        Files.createFile(tempFile3Path);
        Files.write(tempFile3Path, tempFile3String.getBytes());
        
        Files.createDirectories(nestedFolderPath);
        Files.createFile(tempFile4Path);
        Files.write(tempFile4Path, tempFile4String.getBytes());
    }
    
    @AfterClass
    public static void cleanUp() throws IOException {
        //delete temp folder
        if (Files.exists(tempFolderPath)) {
            Files.walk(tempFolderPath)
                .sorted(Collections.reverseOrder())
                .forEach(FileSourceOperatorTest::delete);
        }
    }
    
    /*
     * Test FileSourceOperator with a single file.
     */
    @Test
    public void test1() throws Exception {
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = new FileSourcePredicate(tempFile1Path.toString(), attrName);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();
        
        List<Tuple> expectedResults = Arrays.asList(
                new Tuple(schema, new TextField(tempFile1String)));
        
        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }
    
    /*
     * Test FileSourceOperator with a single file that with an invalid extension (.tmp)
     */
    @Test(expected = RuntimeException.class)
    public void test2() throws Exception {
        String attrName = "content";
        
        FileSourcePredicate predicate = new FileSourcePredicate(tempFile3Path.toString(), attrName);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();        
    }
    
    /*
     * Test FileSourceOperator with a Directory.
     * 
     * Only the files directly under this directory will be used. NOT nested files.
     * For example:
     * tempFolder/
     *   test1.txt
     *   test2.txt
     *   test3.temp (NOT included because of unsupported extension)
     *   nested/
     *     test3.txt (NOT included because of nested folder)
     *     
     * test1.txt and test2.txt will be included, NOT test3. 
     */
    @Test
    public void test3() throws Exception {
        String attrName = "content";
        Schema schema = new Schema(new Attribute(attrName, AttributeType.TEXT));
        
        FileSourcePredicate predicate = new FileSourcePredicate(tempFolderPath.toString(), attrName);
        FileSourceOperator fileSource = new FileSourceOperator(predicate);
        
        Tuple tuple;
        ArrayList<Tuple> exactResults = new ArrayList<>();
        fileSource.open();
        while ((tuple = fileSource.getNextTuple()) != null) {
            exactResults.add(tuple);
        }
        fileSource.close();
                
        List<Tuple> expectedResults = Arrays.asList(
                new Tuple(schema, new TextField(tempFile1String)), 
                new Tuple(schema, new TextField(tempFile2String)));
        
        Assert.assertTrue(TestUtils.equals(expectedResults, exactResults));
    }
    
}
