package edu.uci.ics.textdb.exp.source.asterix;

import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONArray;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class AsterixSource implements ISourceOperator {
    
    public static String RAW_DATA = "rawData";
    public static Attribute RAW_DATA_ATTR = new Attribute(RAW_DATA, AttributeType.TEXT);
    public static Schema ATERIX_SOURCE_SCHEMA = new Schema(SchemaConstants._ID_ATTRIBUTE, RAW_DATA_ATTR);
    
    private final AsterixSourcePredicate predicate;
    private JSONArray resultJsonArray;
    
    private int cursor = CLOSED;
    
    public AsterixSource(AsterixSourcePredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public void open() throws TextDBException {
        if (cursor == OPENED) {
            return;
        }
        try {
            String asterixAddress = "http://" + predicate.getHost() + ":" + predicate.getPort() + 
                    "/query/service";
            String asterixQuery = generateAsterixQuery(predicate);
            HttpResponse<JsonNode> jsonResponse = Unirest.post(asterixAddress)
                    .queryString("statement", asterixQuery)
                    .field("mode", "immediate")
                    .asJson();
            
            // if status is 200 OK, store the results
            if (jsonResponse.getStatus() == 200) {
                this.resultJsonArray = jsonResponse.getBody().getObject().getJSONArray("results");
            } else {
                throw new DataFlowException("Send query to asterix failed: " + 
                        "error status: " + jsonResponse.getStatusText() + ", " + 
                        "error body: " + jsonResponse.getBody().toString());
            }
            cursor = OPENED;
        } catch (UnirestException e) {
            throw new DataFlowException(e);
        }
    }
    
    private static String generateAsterixQuery(AsterixSourcePredicate predicate) {
        String asDataset = "ds";
        StringBuilder sb = new StringBuilder();
        sb.append("use " + predicate.getDataverse() + ';').append("\n");
        sb.append("select * ").append("\n");
        sb.append("from " + predicate.getDataset() + " as " + asDataset).append("\n");
        sb.append("where true").append("\n");
        if (predicate.getField() != null && predicate.getKeyword() != null) {
            List<String> keywordList = DataflowUtils.tokenizeQuery(
                    LuceneAnalyzerConstants.standardAnalyzerString(), predicate.getKeyword());
            String asterixKeyword = 
                    "[" +
                    keywordList.stream().map(keyword -> "\"" + keyword + "\"")
                        .collect(Collectors.joining(", ")) +  
                    "]";
            String asterixField = "`" + predicate.getField() + "`";
            sb.append("and ftcontains(" + asDataset + "." + asterixField + ", ");
            sb.append(asterixKeyword + ", " + "{\"mode\":\"all\"}" + ")").append("\n");
        }
        if(predicate.getStartDate() != null){
        	String startDate = predicate.getStartDate();
        	sb.append("and create_at >= datetime(\""+startDate +"T00:00:04.000Z\")").append("\n");
        }
        if(predicate.getEndDate() != null){
        	String endDate = predicate.getEndDate();
        	
        	sb.append("and create_at <= datetime(\""+endDate +"T00:00:04.000Z\")").append("\n");
        }
        if (predicate.getLimit() != null) {
            sb.append("limit " + predicate.getLimit()).append("\n");
        }
        sb.append(";");
        return sb.toString();
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        if (cursor < resultJsonArray.length()) {
            Tuple tuple =  new Tuple(ATERIX_SOURCE_SCHEMA, 
                    IDField.newRandomID(),
                    new TextField(resultJsonArray.getJSONObject(cursor).get("ds").toString()));
            cursor ++;
            return tuple;
        }
        return null;
    }

    @Override
    public void close() throws TextDBException {
        if (cursor == CLOSED) {
            return;
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return ATERIX_SOURCE_SCHEMA;
    }

}
