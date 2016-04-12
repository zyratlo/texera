package edu.uci.ics.textdb.storage;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.constants.LuceneConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;

public class LuceneDataReader{

    
    private String dataDir;
    private List<Attribute> schema;
    private IndexSearcher indexSearcher;
    private ScoreDoc[] scoreDocs;
    private IndexReader indexReader;
    
    public LuceneDataReader(String dataDirectory, List<Attribute> schema) {
        this.dataDir = dataDirectory;
        this.schema = schema;
    }
    
    public List<ITuple> getTuples(){
        List<ITuple> retList = new ArrayList<ITuple>();
        try {
            Directory directory = FSDirectory.open(Paths.get(dataDir));
            indexReader = DirectoryReader.open(directory);
            indexSearcher = new IndexSearcher(indexReader);
            Analyzer analyzer = new StandardAnalyzer();
            Attribute defaultAttr = schema.get(0);
            QueryParser queryParser = new QueryParser(defaultAttr.getFieldName(), analyzer);
            Query query = queryParser.parse(LuceneConstants.SCAN_QUERY);
            
            TopDocs topDocs = indexSearcher.search(query, Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;
            int numDocs = scoreDocs.length;
            int cursor = 0;
            while(cursor < numDocs){
                Document document = indexSearcher.doc(scoreDocs[cursor++].doc);
                
                List<IField> fields = new ArrayList<IField>();
                for (Attribute  attr : schema) {
                    FieldType fieldType = attr.getFieldType();
                    
                    String fieldValue = document.get(attr.getFieldName());
                    IField field = Utils.getField(fieldType, fieldValue);
                    fields.add(field);
                }
                
                DataTuple dataTuple = new DataTuple(schema, fields.toArray(new IField[fields.size()]));
                retList.add(dataTuple);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        } finally{
            if(indexReader != null){
                try {
                    indexReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return retList;
    }

}
