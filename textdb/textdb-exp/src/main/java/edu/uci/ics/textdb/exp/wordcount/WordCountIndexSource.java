package edu.uci.ics.textdb.exp.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.storage.DataReader;
import edu.uci.ics.textdb.storage.RelationManager;

public class WordCountIndexSource extends AbstractSingleInputOperator implements ISourceOperator{
    private WordCountIndexSourcePredicate predicate;
    
    public static final String WORD = "word";
    public static final String COUNT = "count";
    public static final Attribute WORD_ATTR = new Attribute(WORD, AttributeType.STRING);
    public static final Attribute COUNT_ATTR = new Attribute(COUNT, AttributeType.INTEGER);
    public static final Schema SCHEMA_WORD_COUNT = new Schema(SchemaConstants._ID_ATTRIBUTE, WORD_ATTR, COUNT_ATTR);
    
    private HashMap<String, Integer> wordCountMap = null;
    private Iterator<Entry<String, Integer>> wordCountIterator = null;
    
    public WordCountIndexSource(WordCountIndexSourcePredicate predicate) {
        this.predicate = predicate;
    }
    
    @Override
    protected void setUp() throws DataFlowException {
        this.outputSchema = SCHEMA_WORD_COUNT;
    }
    
    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        if (wordCountMap == null) {
            wordCountMap = new HashMap<String, Integer>();
            computeWordCount();
            wordCountIterator = wordCountMap.entrySet().iterator();
        }        
        if (wordCountIterator.hasNext()) {
            Entry<String, Integer> entry = wordCountIterator.next();
            List<IField> tupleFieldList = new ArrayList<>();
            // Generate the new UUID.
            tupleFieldList.add(IDField.newRandomID());
            tupleFieldList.add(new StringField(entry.getKey()));
            tupleFieldList.add(new IntegerField(entry.getValue()));
            
            return new Tuple(outputSchema, tupleFieldList);
        }
        return null;
    }
    
    private void computeWordCount() throws TextDBException {
        try {
            DataReader dataReader = RelationManager.getRelationManager().getTableDataReader(
                    predicate.getTableName(), new MatchAllDocsQuery());
            
            dataReader.open();
            
            IndexReader luceneIndexReader = dataReader.getLuceneIndexReader();
            
            for (int i = 0; i< luceneIndexReader.numDocs(); i++) {
                Terms termVector = luceneIndexReader.getTermVector(i, predicate.getAttribute());
                
                TermsEnum termsEnum = termVector.iterator();
                while(termsEnum.next() != null){
                    String key = termsEnum.term().utf8ToString();
                    wordCountMap.put(key, wordCountMap.get(key)==null ?
                            ((int) termsEnum.totalTermFreq()) :
                                wordCountMap.get(key) + ((int) termsEnum.totalTermFreq()));
                }
            }
            
            luceneIndexReader.close();
            dataReader.close();
        } catch (IOException e) {
            throw new DataFlowException(e);
        }
        
    }
    
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        throw new DataFlowException("not supported");
    }
    
    @Override
    protected void cleanUp() throws TextDBException {
    }
}
