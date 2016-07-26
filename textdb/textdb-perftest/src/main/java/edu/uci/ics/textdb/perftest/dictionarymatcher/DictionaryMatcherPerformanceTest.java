package edu.uci.ics.textdb.perftest.dictionarymatcher;

import java.io.File;
import java.io.FileWriter;
 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataStore;

/**
 * @author Hailey Pan
 * 
 * This is the performance test of dictionary matcher
 * */

public class DictionaryMatcherPerformanceTest {
	
	private static String HEADER = "Record #,Operator,Dict, Words/Phrase in dict, Time, Total Results\n";
	private static String basicHeader = "Conjunctive,";
	private static String phraseHeader = "Phrase,";
	private static String scanHeader = "Scan,";
	private static String commaDelimiter = ",";
	private static String newLine = "\n";	
	private static FileWriter fileWriter = null;
	private static 	String csvFileFolder= "dictionary/";
	
	
	/**
	 * @param dictFile: this file contains line(s) of phrases/words which used to form a dictionary for matching;
	 * 					the file must be placed in ./data-files/dictionaries/.
	 * @param testCycle: the number of times the test expected to be ran 
	 * @return 
	 * 
	 * This function will match the dictionary against all indices in ./index/standard/
	 * 
	 *Test results includes runtime, the number of results for each operator, each index and each test cycle. 
	 *They are written in a csv file that is named by current time and located at
	 * ./data-files/results/dictionary/.
	 * 
	 * */
	public static void runTest(String dictFile, int testCycle) throws Exception{		
		ArrayList<String> dictionary = PerfTestUtils.readDict(PerfTestUtils.getDictPath(dictFile));
	 
		if(!new File(PerfTestUtils.resultFolder, "dictionary").exists()){
			File resultFile = new File(PerfTestUtils.resultFolder+csvFileFolder);
			resultFile.mkdir();
		}
		
		String currentTime = PerfTestUtils.formatTime(System.currentTimeMillis());
		String csvFile = csvFileFolder+currentTime+".csv";
		fileWriter = new FileWriter(PerfTestUtils.getResultPath(csvFile));
		
		
		File indexFiles = new File(PerfTestUtils.standardIndexFolder);
		for(int i = 1; i <= testCycle; i++){
			fileWriter.append("Cycle" +i);
			fileWriter.append(newLine );
			fileWriter.append(HEADER);
			for(File file: indexFiles.listFiles()){
				if (file.getName().startsWith(".")) {
					continue;
				}
				DataStore dataStore = new DataStore(PerfTestUtils.getIndexPath(file.getName()), MedlineIndexWriter.SCHEMA_MEDLINE);
				fileWriter.append(file.getName()+",");
				fileWriter.append(basicHeader);
				fileWriter.append(dictFile+commaDelimiter);
				fileWriter.append(Integer.toString(dictionary.size()) + commaDelimiter);
				match(dictionary, DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED, new StandardAnalyzer(), dataStore);	
				
				fileWriter.append(file.getName()+",");
				fileWriter.append(phraseHeader); 
				fileWriter.append(dictFile+commaDelimiter);
				fileWriter.append(Integer.toString(dictionary.size()) + commaDelimiter);
				match(dictionary, DataConstants.KeywordMatchingType.PHRASE_INDEXBASED, new StandardAnalyzer(), dataStore);
				
				fileWriter.append(file.getName()+",");
				fileWriter.append(scanHeader); 
				fileWriter.append(dictFile+commaDelimiter);
				fileWriter.append(Integer.toString(dictionary.size()) + commaDelimiter);
				match(dictionary, DataConstants.KeywordMatchingType.SUBSTRING_SCANBASED, new StandardAnalyzer(), dataStore);	
			}
			fileWriter.append(newLine);
		}
		
		 
		fileWriter.flush();
	    fileWriter.close();	
	}
	
	
	/**
	 * @param dict:  dictionary
	 * @param Optye: operator type
	 * @param luceneAnalyzer
	 * @param DataStore
	 * @return
	 * 
	 * This function does match for a dictionary
	 * */
	public static void match(ArrayList<String> dict, KeywordMatchingType Optype,Analyzer luceneAnalyzer, DataStore dataStore) throws Exception{
		List<Attribute> attributes = Arrays.asList(MedlineIndexWriter.ABSTRACT_ATTR);
		 
		IDictionary dictionary = new Dictionary(dict);
    	IPredicate dictionaryPredicate = new DictionaryPredicate(dictionary, luceneAnalyzer, attributes, Optype, dataStore);
    	DictionaryMatcher dictionaryMatcher = new DictionaryMatcher(dictionaryPredicate);

    	long startMatchTime = System.currentTimeMillis();
    	dictionaryMatcher.open();
        ITuple nextTuple = null;
        int counter = 0;
        while ((nextTuple = dictionaryMatcher.getNextTuple()) != null) {
			List<Span> spanList = ((ListField<Span>) nextTuple.getField(SchemaConstants.SPAN_LIST)).getValue();
			counter += spanList.size();
        }
        dictionaryMatcher.close();
		long endMatchTime = System.currentTimeMillis();
		double matchTime = (endMatchTime - startMatchTime)/1000.0;
		
		fileWriter.append(String.format("%.4f secs", matchTime)+ commaDelimiter );
		fileWriter.append(Integer.toString(counter));
		fileWriter.append(newLine);
	}

	 
}
