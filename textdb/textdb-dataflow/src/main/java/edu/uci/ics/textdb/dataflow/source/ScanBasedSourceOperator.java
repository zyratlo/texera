package edu.uci.ics.textdb.dataflow.source;

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

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.common.StringField;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.dataflow.common.DataTuple;
import edu.uci.ics.textdb.dataflow.constants.LuceneConstants;

/**
 * Created by chenli on 3/28/16.
 */
public class ScanBasedSourceOperator implements ISourceOperator {
    
    private String dataDir;
    private List<String> schema;
    private int cursor = -1;
    private IndexSearcher indexSearcher;
    private ScoreDoc[] scoreDocs;
    private IndexReader indexReader;
    
    public ScanBasedSourceOperator(String dataDirectory, List<String> schema) {
        this.dataDir = dataDirectory;
        this.schema = schema;
    }

    @Override
    public void open() throws DataFlowException {

        try {
            Directory directory = FSDirectory.open(Paths.get(dataDir));
            indexReader = DirectoryReader.open(directory);
            indexSearcher = new IndexSearcher(indexReader);
            Analyzer analyzer = new StandardAnalyzer();
            //TODO ideally default field should be combination of all the fields
            String defaultField = schema.get(0);
            QueryParser queryParser = new QueryParser(defaultField, analyzer);
            Query query = queryParser.parse(LuceneConstants.SCAN_QUERY);
            
            TopDocs topDocs = indexSearcher.search(query, Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;
            cursor = 0;
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
        if(cursor == -1){
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            if(cursor >= scoreDocs.length){
                return null;
            }
            Document document = indexSearcher.doc(scoreDocs[cursor++].doc);
            List<IField> fields = new ArrayList<IField>();
            for (String fieldName : schema) {
                //TODO Generalize it to all field types(int, date etc)
                IField field = new StringField(document.get(fieldName));
                fields.add(field);
            }
            
            DataTuple dataTuple = new DataTuple(schema, fields.toArray(new IField[fields.size()]));
            return dataTuple;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        
    }

    @Override
    public void close() throws DataFlowException {
        cursor = -1;
        if(indexReader != null){
            try {
                indexReader.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new DataFlowException(e.getMessage(), e);
            }
        }
        
    }
}
