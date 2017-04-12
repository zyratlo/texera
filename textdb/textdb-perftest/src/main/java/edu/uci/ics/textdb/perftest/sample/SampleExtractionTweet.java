package edu.uci.ics.textdb.perftest.sample;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.engine.Engine;
import edu.uci.ics.textdb.api.engine.Plan;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.dataflow.utils.DataflowUtils;
import edu.uci.ics.textdb.perftest.twitter.TwitterSchema;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.json.JSONObject;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;

public class SampleExtractionTweet {
    
    public static final String TWITTER_SAMPLE_TABLE = "twitter";
        
    public static String twitterFilesDirectory;
    public static String twitterIndexDirectory;
    public static String sampleDataFilesDirectory;

    static {
        try {
            // Finding the absolute path to the sample data files directory and index directory

            // Checking if the resource is in a jar
            String referencePath = SampleExtractionTweet.class.getResource("").toURI().toString();
            if(referencePath.substring(0, 3).equals("jar")) {
                twitterFilesDirectory = "../textdb-perftest/src/main/resources/sample-data-files/Twitter/sample_twitter_10K.json";
                twitterIndexDirectory = "../textdb-perftest/src/main/resources/index/standard/Twitter/";
                sampleDataFilesDirectory = "../textdb-perftest/src/main/resources/sample-data-files/";
            }
            else {
                twitterFilesDirectory = Paths.get(SampleExtractionTweet.class.getResource("/sample-data-files/Twitter/sample_twitter_10K.json")
                        .toURI())
                        .toString();
                twitterIndexDirectory = Paths.get(SampleExtractionTweet.class.getResource("/index/standard")
                        .toURI())
                        .toString() + "/Twitter";
                sampleDataFilesDirectory = Paths.get(SampleExtractionTweet.class.getResource("/sample-data-files")
                        .toURI())
                        .toString();
            }
        }
        catch(URISyntaxException | FileSystemNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception {
        // write the index of data files
        // index only needs to be written once, after the first run, this function can be commented out
        writeSampleIndex();

        // perform the extraction task
        extractPersonLocation();
    }

//    public static Tuple parsePromedHTML(String fileName, String content) {
//        try {
//            Document parsedDocument = Jsoup.parse(content);
//            String mainText = parsedDocument.getElementById("preview").text();
//            Tuple tuple = new Tuple(PromedSchema.PROMED_SCHEMA, new StringField(fileName), new TextField(mainText));
//            return tuple;
//        } catch (Exception e) {
//            return null;
//        }
//    }

    public static void writeSampleIndex() throws Exception {

        // parse the original file
        File sourceFile = new File(twitterFilesDirectory);
        Scanner scanner= new Scanner(sourceFile);
        ArrayList<Tuple> fileTuples = new ArrayList<>();
        while(scanner.hasNext()){
            String s= scanner.nextLine();
            JSONObject jsonObject= new JSONObject(s);
            String ID = jsonObject.getString("id_str");
            String text= jsonObject.getString("text");
            Tuple tuple= new Tuple(edu.uci.ics.textdb.perftest.twitter.TwitterSchema.TWEET_SCHEMA,new StringField(ID),new TextField(text));
            if(tuple!=null){
                fileTuples.add(tuple);
            }
        }


        // write tuples into the table
        RelationManager relationManager = RelationManager.getRelationManager();
        
        relationManager.deleteTable(TWITTER_SAMPLE_TABLE);
        relationManager.createTable(TWITTER_SAMPLE_TABLE, twitterIndexDirectory,
                edu.uci.ics.textdb.perftest.twitter.TwitterSchema.TWEET_SCHEMA, LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(TWITTER_SAMPLE_TABLE);
        dataWriter.open();
        for (Tuple tuple : fileTuples) {
            dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
    }

    /*
     * This is the DAG of this extraction plan.
     * 
     * 
     *              KeywordSource (zika)
     *                       ↓
     *              Projection (content)
     *                  ↓          ↓
     *       regex (a...man)      NLP (location)
     *                  ↓          ↓     
     *             Join (distance < 100)
     *                       ↓
     *              Projection (spanList)
     *                       ↓
     *                    FileSink
     *                    
     */
    public static void extractPersonLocation() throws Exception {
                
        String keywordZika = "climate";
        KeywordPredicate keywordPredicateZika = new KeywordPredicate(keywordZika, Arrays.asList(edu.uci.ics.textdb.perftest.twitter.TwitterSchema.CONTENT),
                new StandardAnalyzer(), KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        KeywordMatcherSourceOperator keywordSource = new KeywordMatcherSourceOperator(
                keywordPredicateZika, TWITTER_SAMPLE_TABLE);
        NlpPredicate nlpPredicateSentiment = new NlpPredicate(NlpPredicate.NlpTokenType.Sentiment, Arrays.asList(TwitterSchema.CONTENT));
        NlpExtractor nlpExtractorSentiment = new NlpExtractor(nlpPredicateSentiment);
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");
        FileSink fileSink = new FileSink(
                new File(sampleDataFilesDirectory + "/person-location-result-"
                        + sdf.format(new Date(System.currentTimeMillis())).toString() + ".txt"));

        fileSink.setToStringFunction((tuple -> DataflowUtils.getTupleString(tuple)));
//
//
//        ProjectionPredicate projectionPredicateIdAndContent = new ProjectionPredicate(
//                Arrays.asList(SchemaConstants._ID, TwitterSchema.ID, TwitterSchema.CONTENT));
//
//        ProjectionOperator projectionOperatorIdAndContent1 = new ProjectionOperator(projectionPredicateIdAndContent);
//        ProjectionOperator projectionOperatorIdAndContent2 = new ProjectionOperator(projectionPredicateIdAndContent);
//
//        String regexPerson = "\\b(A|a|(an)|(An)) .{1,40} ((woman)|(man))\\b";
//        RegexPredicate regexPredicatePerson = new RegexPredicate(regexPerson, Arrays.asList(TwitterSchema.CONTENT),
//                LuceneAnalyzerConstants.getNGramAnalyzer(3));
//        RegexMatcher regexMatcherPerson = new RegexMatcher(regexPredicatePerson);
//
//        NlpPredicate nlpPredicateLocation = new NlpPredicate(NlpPredicate.NlpTokenType.Sentiment, Arrays.asList(TwitterSchema.CONTENT));
//        NlpExtractor nlpExtractorLocation = new NlpExtractor(nlpPredicateLocation);
//
//        IJoinPredicate joinPredicatePersonLocation = new JoinDistancePredicate(TwitterSchema.CONTENT, 100);
//        Join joinPersonLocation = new Join(joinPredicatePersonLocation);
//
//        ProjectionPredicate projectionPredicateIdAndSpan = new ProjectionPredicate(
//                Arrays.asList(SchemaConstants._ID, TwitterSchema.ID, SchemaConstants.SPAN_LIST));
//        ProjectionOperator projectionOperatorIdAndSpan = new ProjectionOperator(projectionPredicateIdAndSpan);
//
//        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");
//        FileSink fileSink = new FileSink(
//                new File(sampleDataFilesDirectory + "/person-location-result-"
//                		+ sdf.format(new Date(System.currentTimeMillis())).toString() + ".txt"));
//
//        fileSink.setToStringFunction((tuple -> DataflowUtils.getTupleString(tuple)));


//        projectionOperatorIdAndContent1.setInputOperator(keywordSource);
//
//        regexMatcherPerson.setInputOperator(projectionOperatorIdAndContent1);
//
//        projectionOperatorIdAndContent2.setInputOperator(regexMatcherPerson);
        nlpExtractorSentiment.setInputOperator(keywordSource);

//        joinPersonLocation.setInnerInputOperator(regexMatcherPerson);
//        joinPersonLocation.setOuterInputOperator(nlpExtractorLocation);
//
//        projectionOperatorIdAndSpan.setInputOperator(joinPersonLocation);
        fileSink.setInputOperator(nlpExtractorSentiment);

        Plan extractPersonPlan = new Plan(fileSink);
        Engine.getEngine().evaluate(extractPersonPlan);
    }

}
