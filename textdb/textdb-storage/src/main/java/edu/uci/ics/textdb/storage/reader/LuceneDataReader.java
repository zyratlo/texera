/**
 * 
 */
package edu.uci.ics.textdb.storage.reader;

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
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;

/**
 * @author sandeepreddy602
 *
 */
public class LuceneDataReader implements IDataReader{
    
    private String dataDir;
    private List<Attribute> schema;
    private int cursor = -1;
    private IndexSearcher indexSearcher;
    private ScoreDoc[] scoreDocs;
    private IndexReader indexReader;
    private String query;
    private String defaultField;
    
    public LuceneDataReader(String dataDirectory, List<Attribute> schema, 
            String query, String defaultField) {
        this.dataDir = dataDirectory;
        this.schema = schema;
        this.query = query;
        this.defaultField = defaultField;
    }
    
    @Override
    public void open() throws DataFlowException {


        try {
            Directory directory = FSDirectory.open(Paths.get(dataDir));
            indexReader = DirectoryReader.open(directory);
            indexSearcher = new IndexSearcher(indexReader);
            Analyzer analyzer = new StandardAnalyzer();
            QueryParser queryParser = new QueryParser(defaultField, analyzer);
            Query queryObj = queryParser.parse(query);
            
            TopDocs topDocs = indexSearcher.search(queryObj, Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;
            cursor = OPENED;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }                
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
        if(cursor == CLOSED){
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            if(cursor >= scoreDocs.length){
                return null;
            }
            Document document = indexSearcher.doc(scoreDocs[cursor++].doc);
            
            List<IField> fields = new ArrayList<IField>();
            for (Attribute  attr : schema) {
                FieldType fieldType = attr.getFieldType();
                String fieldValue = document.get(attr.getFieldName());
                fields.add(Utils.getField(fieldType, fieldValue));
            }
            
            DataTuple dataTuple = new DataTuple(schema, fields.toArray(new IField[fields.size()]));
            return dataTuple;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        
    }

    @Override
    public void close() throws Exception {
        cursor = CLOSED;
        if(indexReader != null){
            try {
                indexReader.close();
                indexReader = null;
            } catch (IOException e) {
                e.printStackTrace();
                throw new DataFlowException(e.getMessage(), e);
            }
        }
    }
    
}
