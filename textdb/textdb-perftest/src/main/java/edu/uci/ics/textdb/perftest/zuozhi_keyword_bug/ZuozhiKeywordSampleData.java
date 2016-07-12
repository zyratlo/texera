package edu.uci.ics.textdb.perftest.zuozhi_keyword_bug;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;
import edu.uci.ics.textdb.perftest.medline.MedlineReader;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.writer.DataWriter;

public class ZuozhiKeywordSampleData {
	

	public static void main(String[] args) throws Exception {
		String query = "and tall a angry";
		writeData();
		
		KeywordPredicate keywordPredicate = 
				new KeywordPredicate(
						query, 
						Arrays.asList(TestConstants.DESCRIPTION_ATTR), 
						DataConstants.KeywordOperatorType.BASIC,
						new StandardAnalyzer(),
						new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE));
		
		KeywordMatcher keywordMatcher = 
				new KeywordMatcher(keywordPredicate);
		
		keywordMatcher.open();
		
		ArrayList<ITuple> tupleList = new ArrayList<>();
		ITuple tuple = null;
		int count = 0;
		while ((tuple = keywordMatcher.getNextTuple()) != null) {
			count++;
//			List<Span> a = ((ListField<Span>) tuple.getField("spanList")).getValue();
//			for (Span i : a) {
//				System.out.printf("fieldName: %s, fieldValue: %s\n",
//						i.getFieldName(), tuple.getField(i.getFieldName()).getValue());
//				System.out.printf("start: %d, end: %d, fieldName: %s, key: %s, value: %s\n", 
//						i.getStart(), i.getEnd(), i.getFieldName(), i.getKey(), i.getValue());
//			}
			tupleList.add(tuple);
		}
		
		System.out.println("count: "+count);

		System.out.println("\n\n------------------\nRESULT!!!");
		printResults(tupleList);
		
		keywordMatcher.close();

	}
	
	private static void writeData() throws Exception {
		DataStore dataStore = new DataStore(DataConstants.INDEX_DIR, TestConstants.SCHEMA_PEOPLE);
        Analyzer luceneAnalyzer = new StandardAnalyzer();
        DataWriter dataWriter = new DataWriter(dataStore, luceneAnalyzer);
        dataWriter.clearData();
        dataWriter.writeData(TestConstants.getSamplePeopleTuples());
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
