
package edu.uci.ics.textdb.perftest.keywordmatcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * @author Hailey Pan
 * 
 *         This is the performance test of KeywordMatcher
 */

public class KeywordMatcherPerformanceTest {

    private static String HEADER = "Date,Record #,Min Time, Max Time, Average Time, Std, Average Results\n";
    private static String delimiter = ",";
    private static String newLine = "\n";
   
   
    private static List<Double> timeResults = null;
    private static int totalResultCount = 0;
    private static String currentTime = "";
    
    //result files
    private static String conjunctiveCsv = "keyword-conjunctive.csv";
    private static String scanCsv = "keyword-scan.csv";
    private static String phraseCsv = "keyword-phrase.csv";

    /**
     * @param queryFileName:
     *            this file contains line(s) of queries; the file must be placed
     *            in ./data-files/queries/
     * @param iterationNumber:
     *            the number of times the test expected to be run
     * @return
     * 
     *         This function will match the queries against all indices in
     *         ./index/standard/
     * 
     *         Test results for each operator include minimum runtime, maximum runtime, average
     *         runtime, the standard deviation and the average number of results are recorded in corresponding 
     *         csv files: ./data-files/results/keyword-conjunctive
     *         		      ./data-files/results/keyword-phrase
     *                    ./data-files/results/keyword-scan			 
     *         
     * @throws Exception 
     * 
     */
    public static void runTest(String queryFileName)
            throws Exception {

        // Reads queries from query file into a list
        ArrayList<String> queries = PerfTestUtils.readQueries(PerfTestUtils.getQueryPath(queryFileName));
 
        // Gets the current time  
        currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());

        File indexFiles = new File(PerfTestUtils.standardIndexFolder);
    
        // Does match against each index in ./index/
        for (File file : indexFiles.listFiles()) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            DataStore dataStore = new DataStore(PerfTestUtils.getIndexPath(file.getName()),
                    MedlineIndexWriter.SCHEMA_MEDLINE);
            
            csvWriter(conjunctiveCsv, file.getName(), queries, DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED, dataStore);
            csvWriter(phraseCsv, file.getName(), queries, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, dataStore);
            csvWriter(scanCsv, file.getName(), queries, DataConstants.KeywordMatchingType.SUBSTRING_SCANBASED, dataStore);

        }
        
    }
    
    /**
     * 
     * @param resultFile
     * @param recordNum
     * @param queries
     * @param opType
     * @param dataStore
     * @throws Exception
     * 
     * This function writes test results to the given csv file.
     */
    public static void csvWriter(String resultFile, String recordNum, ArrayList<String> queries, 
    		KeywordMatchingType opType, DataStore dataStore) throws Exception {
    	double avgTime = 0.0;
    	PerfTestUtils.createFile(PerfTestUtils.getResultPath(resultFile), HEADER);
    	FileWriter fileWriter = new FileWriter(PerfTestUtils.getResultPath(resultFile),true);
    	fileWriter.append(currentTime + delimiter);
    	fileWriter.append(recordNum + delimiter);
    	resetStats();
    	match(queries, opType, new StandardAnalyzer(), dataStore);
    	avgTime = PerfTestUtils.calculateAverage(timeResults);
    	fileWriter.append(Collections.min(timeResults) + delimiter + Collections.max(timeResults) + delimiter + 
     		avgTime + delimiter + PerfTestUtils.calculateSTD(timeResults, avgTime) + delimiter
             + String.format("%.2f", totalResultCount * 1.0 / queries.size()));
    	fileWriter.append(newLine);
    	fileWriter.flush();
     	fileWriter.close();
    }

    /**
     * reset timeResults and totalResultCount
     */
    public static void resetStats() {
        timeResults = new ArrayList<Double>();
        totalResultCount = 0;
    }

    /**
     * @param queryList:
     *            queries
     * @param Optype:
     *            operator type
     * @param luceneAnalyzer
     * @param DataStore
     * @return
     * 
     *         This function does match for a list of queries
     */
    public static void match(ArrayList<String> queryList, KeywordMatchingType opType, Analyzer luceneAnalyzer,
            DataStore dataStore) throws DataFlowException, IOException {

        Attribute[] attributeList = new Attribute[] { MedlineIndexWriter.ABSTRACT_ATTR };

        for (String query : queryList) {
            KeywordPredicate keywordPredicate = new KeywordPredicate(query, Arrays.asList(attributeList),
                    luceneAnalyzer, opType);
            IndexBasedSourceOperator indexInputOperator = new IndexBasedSourceOperator(
                    keywordPredicate.generateDataReaderPredicate(dataStore));
            KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
            keywordMatcher.setInputOperator(indexInputOperator);

            long startMatchTime = System.currentTimeMillis();
            keywordMatcher.open();
            int counter = 0;
            ITuple nextTuple = null;
            while ((nextTuple = keywordMatcher.getNextTuple()) != null) {
                List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
                counter += spanList.size();
            }
            keywordMatcher.close();
            long endMatchTime = System.currentTimeMillis();
            double matchTime = (endMatchTime - startMatchTime) / 1000.0;

            timeResults.add(Double.parseDouble(String.format("%.4f", matchTime)));
            totalResultCount += counter;

        }
    }

}
