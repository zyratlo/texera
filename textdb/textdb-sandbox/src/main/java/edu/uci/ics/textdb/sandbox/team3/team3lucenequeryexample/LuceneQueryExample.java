package edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.storage.LuceneDataStore;
import edu.uci.ics.textdb.storage.writer.LuceneDataWriter;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.api.storage.IDataWriter;

/* 
 * This example shows that lucene can process manually translated binary expressions.  
 */

public class LuceneQueryExample {
	public String dataFileName;
	
	private final Schema schema = LuceneQueryExampleConstants.SCHEMA_DOC;
	private List<ITuple> dataTuples;
	
	private Analyzer analyzer;
	private IndexSearcher searcher;
	private IDataStore dataStore;
	private IDataWriter dataWriter;
	
	public LuceneQueryExample(String dfn, int min_ngram_size, int max_ngram_size) throws Exception {
		dataFileName = dfn;
		dataTuples = new ArrayList<ITuple>();
		analyzer = new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				Tokenizer source = new NGramTokenizer(min_ngram_size, max_ngram_size);
				return new TokenStreamComponents(source);
			}
		};
		
		dataStore = new LuceneDataStore(LuceneConstants.INDEX_DIR, schema);
		dataWriter = new LuceneDataWriter(dataStore, analyzer);
	}
	
	public void buildNGramIndex() throws Exception {
		getITupleListFromDataFile();
		dataWriter.clearData();
		dataWriter.writeData(dataTuples);
	}
	
	private void getITupleListFromDataFile() throws Exception {
		Scanner fileScanner = new Scanner(new File(dataFileName));
		Integer lineNum = 1;
		dataTuples.clear();
		
		while (fileScanner.hasNextLine()) {
			IField[] fields = new IField[schema.getAttributes().size()];
			
			FieldType type = new FieldType();
			type.setIndexOptions(IndexOptions.DOCS);
			type.setStored(true);
			type.setStoreTermVectors(true);
			String nextline = fileScanner.nextLine();
			IField dataField = new StringField(nextline);
			IField idField = new IntegerField(lineNum);
			
			fields[0] = idField;
			fields[1] = dataField;
			
			ITuple tuple = new DataTuple(schema, fields);
			dataTuples.add(tuple);
			
			lineNum++;
		}
		fileScanner.close();
	}
	
	public TopDocs search(String queryText, int numOfTopHit) throws Exception{
//		Directory directory = FSDirectory.open(Paths.get(dataStore.getDataDirectory()));
//        IndexReader indexReader = DirectoryReader.open(directory);
//        Terms terms = indexReader.getTermVector(0, "data");
//        if (terms != null) {
//        	TermsEnum iter = terms.iterator();
//        	String termText = iter.term().utf8ToString();
//        	System.out.println(termText);
//        } else {
//        	System.out.println("Index is null!!!!!");
//        }
//        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
//		TopDocs res = indexSearcher.search(query, numOfTopHit);
		searcher = new IndexSearcher(
				DirectoryReader.open(FSDirectory.open(Paths.get(LuceneConstants.INDEX_DIR))));
		
        QueryParser parser = new QueryParser("data", analyzer);
		Query query = parser.parse(queryText);
		TopDocs res = searcher.search(query, numOfTopHit);

		
//		System.out.println("Top hits = " + res.totalHits);
		return res;

	}
	
	public IndexSearcher getSearcher() {
		return searcher;
	}
}
