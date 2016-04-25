package edu.uci.ics.textdb.sandbox.team3.team3lucenequeryexample;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class LuceneQueryExample {
	String dataFileName;
	Analyzer analyzer;
	IndexSearcher searcher;
	
	public LuceneQueryExample(String dfn, int n_min, int n_max) {
		dataFileName = dfn;
		
		analyzer = new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				Tokenizer source = new NGramTokenizer(n_min, n_max);
				return new TokenStreamComponents(source);
			}
		};
	}
	
	public void initiateSearcher() throws Exception {
		searcher = new IndexSearcher(
				DirectoryReader.open(FSDirectory.open(Paths.get("index"))));
	}
	
	public IndexSearcher getSearcher() {
		return searcher;
	}
	
	public void buildNGramIndex() throws Exception {
		Directory indexDir = FSDirectory.open(Paths.get("index"));
		try {
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			IndexWriter writer = new IndexWriter(indexDir, config);
			Scanner dataFile = new Scanner(new File(dataFileName));
			Integer lineNum = 1;
			while (dataFile.hasNextLine()) {
				Document doc = new Document();
				FieldType type = new FieldType();
				type.setIndexOptions(IndexOptions.DOCS);
				type.setStored(true);
				type.setStoreTermVectors(true);
				Field dataField = new Field("data", dataFile.nextLine(), type);
				Field idField = new Field("id", lineNum.toString(), type);
				doc.add(dataField);
				doc.add(idField);
				writer.addDocument(doc);
				lineNum++;
			}
			dataFile.close();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public TopDocs search(String queryText, int n) throws Exception{
		
		QueryParser parser = new QueryParser("data", analyzer);
		Query query = parser.parse(queryText);
		TopDocs res = searcher.search(query, n);
		return res;

	}
}
