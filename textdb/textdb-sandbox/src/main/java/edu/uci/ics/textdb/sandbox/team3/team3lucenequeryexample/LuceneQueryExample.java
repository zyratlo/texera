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
	public static void main(String[] args) throws Exception {
		String dataFileName = "/Users/laishuying/Desktop/lucenetext.txt";
		
		Analyzer analyzer = new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				Tokenizer source = new NGramTokenizer(3,3);
				return new TokenStreamComponents(source);
			}
		};
		
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
		
		//perform search "network"
		String querytext = "data:\"net\" AND data:\"etw\" AND data:\"two\" AND data:\"wor\" AND data: \"ork\" ";
		IndexSearcher searcher = new IndexSearcher(
				DirectoryReader.open(FSDirectory.open(Paths.get("index"))));
		QueryParser parser = new QueryParser("data", analyzer);
		Query query = parser.parse(querytext);
		TopDocs res = searcher.search(query, 100);
		ScoreDoc[] scoredocs= res.scoreDocs;
		for (ScoreDoc scoredoc: scoredocs) {
			System.out.println("Hit document id: " + scoredoc.doc);
			Document document = searcher.doc(scoredoc.doc);
			String text = document.getField("data").stringValue();
			System.out.println("Hit document text: " + text);
		}
	}
}
