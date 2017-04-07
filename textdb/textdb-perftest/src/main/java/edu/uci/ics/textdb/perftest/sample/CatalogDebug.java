package edu.uci.ics.textdb.perftest.sample;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;
import edu.uci.ics.textdb.perftest.promed.PromedSchema;
import edu.uci.ics.textdb.storage.CatalogConstants;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class CatalogDebug {
    
    public static final String PROMED_SAMPLE_TABLE = "zhang";
    
    public static final String PROMED_BUG_TABLE = "zhang0";
        
    public static String promedFilesDirectory;
    public static String promedIndexDirectory;
    public static String sampleDataFilesDirectory;

    static {
        try {
            // Finding the absolute path to the sample data files directory and index directory

            // Checking if the resource is in a jar
            String referencePath = SampleExtraction.class.getResource("").toURI().toString();
            if(referencePath.substring(0, 3).equals("jar")) {
                promedFilesDirectory = "../textdb-perftest/src/main/resources/sample-data-files/promed/";
                promedIndexDirectory = "../textdb-perftest/src/main/resources/index/standard/promed/";
                sampleDataFilesDirectory = "../textdb-perftest/src/main/resources/sample-data-files/";
            }
            else {
                promedFilesDirectory = Paths.get(SampleExtraction.class.getResource("/sample-data-files/promed")
                        .toURI())
                        .toString();
                promedIndexDirectory = Paths.get(SampleExtraction.class.getResource("/index/standard")
                        .toURI())
                        .toString() + "/promed";
                sampleDataFilesDirectory = Paths.get(SampleExtraction.class.getResource("/sample-data-files")
                        .toURI())
                        .toString();
            }
        }
        catch(URISyntaxException | FileSystemNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception {
        bugFunction();

    }
    
    public static void printCatalog() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        
        DataReader dataReader = relationManager.getTableDataReader(CatalogConstants.TABLE_CATALOG, new MatchAllDocsQuery());
        dataReader.open();
        Tuple tuple;
        List<Tuple> tableCatalogList = new ArrayList<>();
        while ((tuple = dataReader.getNextTuple()) != null) {
            tableCatalogList.add(tuple);
        }
        dataReader.close();
        
        dataReader = relationManager.getTableDataReader(CatalogConstants.SCHEMA_CATALOG, new MatchAllDocsQuery());
        dataReader.open();
        List<Tuple> schemaCatalogList = new ArrayList<>();
        while ((tuple = dataReader.getNextTuple()) != null) {
            schemaCatalogList.add(tuple);
        }
        dataReader.close();
        
        System.out.println("table catalog");
        System.out.println(DataflowUtils.getTupleListString(tableCatalogList));
        System.out.println("schema catalog");
        System.out.println(DataflowUtils.getTupleListString(schemaCatalogList));
    }

    public static Tuple parsePromedHTML(String fileName, String content) {
        try {
            Document parsedDocument = Jsoup.parse(content);
            String mainText = parsedDocument.getElementById("preview").text();
            Tuple tuple = new Tuple(PromedSchema.PROMED_SCHEMA, new StringField(fileName), new TextField(mainText));
            return tuple;
        } catch (Exception e) {
            return null;
        }
    }
    
    public static void bugFunction() throws Exception {
        // parse the original file
        File sourceFileFolder = new File(promedFilesDirectory);
        ArrayList<Tuple> fileTuples = new ArrayList<>();
        for (File htmlFile : sourceFileFolder.listFiles()) {
            StringBuilder sb = new StringBuilder();
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                sb.append(scanner.nextLine());
            }
            scanner.close();
            Tuple tuple = parsePromedHTML(htmlFile.getName(), sb.toString());
            if (tuple != null) {
                fileTuples.add(tuple);
            }
        }
        
        // write tuples into the table
        RelationManager relationManager = RelationManager.getRelationManager();
        
        System.out.println(1);
        printCatalog();
        relationManager.deleteTable(PROMED_SAMPLE_TABLE);
        System.out.println(2);
        printCatalog();
        relationManager.createTable(PROMED_SAMPLE_TABLE, promedIndexDirectory, 
                PromedSchema.PROMED_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
        System.out.println(3);
        printCatalog();
        
        DataWriter dataWriter = relationManager.getTableDataWriter(PROMED_SAMPLE_TABLE);
        dataWriter.open();
        for (Tuple tuple : fileTuples) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
        
        
        relationManager.deleteTable(PROMED_SAMPLE_TABLE);
        System.out.println(4);
        printCatalog();
        
        relationManager.deleteTable(PROMED_BUG_TABLE);
        System.out.println(5);
        printCatalog();
        relationManager.createTable(PROMED_BUG_TABLE, promedIndexDirectory,
                PromedSchema.PROMED_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
        System.out.println(6);
        printCatalog();
        
        DataWriter dataWriterBug = relationManager.getTableDataWriter(PROMED_BUG_TABLE);
        dataWriterBug.open();
        for (int i = 0; i < 2; i++) {
            Tuple tuple = new Tuple(PromedSchema.PROMED_SCHEMA, new StringField("title"+i), new TextField("content"));
            dataWriterBug.insertTuple(tuple);
        }
        dataWriterBug.close();

    }
}
