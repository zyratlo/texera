package edu.uci.ics.textdb.perftest.zuozhi_dict_bug;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.DictionaryOperatorType;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.medline.MedlineReader;
import edu.uci.ics.textdb.storage.DataStore;

public class ZuozhiDictTweets {
	

	public static void main(String[] args) throws Exception {
		
		DataStore dataStore = new DataStore("./index/dict_twitter_10K/", TweetsConstants.SCHEMA_TWEETS);		
//		TweetsIndexWriter.writeTweetsToIndex("./data-files/sampleTweets_10K.json", dataStore, new StandardAnalyzer());
		
		ArrayList<String> dictList = new ArrayList<>();
		
		dictList.add("zika-virus");
		IDictionary dictionary = new Dictionary(dictList);

		DictionaryPredicate dictionaryPredicate = 
				new DictionaryPredicate(
						dictionary, 
						new StandardAnalyzer(),
						Arrays.asList(TweetsConstants.TEXT_ATTR),
						DictionaryOperatorType.KEYWORD_PHRASE,
						dataStore);
		
		DictionaryMatcher dictionaryMatcher = 
				new DictionaryMatcher(dictionaryPredicate);
		
		dictionaryMatcher.open();
		
		ArrayList<ITuple> tupleList = new ArrayList<>();
		ITuple tuple = null;
		int count = 0;
		while ((tuple = dictionaryMatcher.getNextTuple()) != null) {
			count++;
			System.out.println("count: "+count);
			List<Span> a = ((ListField<Span>) tuple.getField("spanList")).getValue();
			for (Span i : a) {
				System.out.printf("fieldName: %s, fieldValue: %s\n",
						i.getFieldName(), tuple.getField(i.getFieldName()).getValue());
				System.out.printf("start: %d, end: %d, fieldName: %s, key: %s, value: %s\n", 
						i.getStart(), i.getEnd(), i.getFieldName(), i.getKey(), i.getValue());
			}			tupleList.add(tuple);
		}
		
		printResults(tupleList);
		
		dictionaryMatcher.close();

	}
	
	private static void writeData(String filePath) throws StorageException, FileNotFoundException {
		DataStore dataStore = new DataStore(DataConstants.INDEX_DIR, MedlineReader.SCHEMA_MEDLINE);
        Analyzer luceneAnalyzer = new StandardAnalyzer();
		MedlineIndexWriter.writeMedlineToIndex(filePath, dataStore, luceneAnalyzer);

	}
	
	// Helper function to print results for debugging purposes
	public static void printResults(List<ITuple> results) {
		for (ITuple result : results) {
			List<Span> a = ((ListField<Span>) result.getField("spanList")).getValue();
			for (Span i : a) {
				System.out.printf("fieldName: %s, fieldValue: %s\n",
						i.getFieldName(), result.getField(i.getFieldName()).getValue());
				System.out.printf("start: %d, end: %d, fieldName: %s, key: %s, value: %s\n", 
						i.getStart(), i.getEnd(), i.getFieldName(), i.getKey(), i.getValue());
			}
		}
		System.out.println();
	}
	
}
