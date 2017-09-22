package edu.uci.ics.texera.dataflow.operatorstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;

import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordPredicate;
import edu.uci.ics.texera.dataflow.keywordmatcher.KeywordSourcePredicate;

public class JsonSchemaHelper {
    
    public static void main(String[] args) throws Exception {
        generateJsonSchema();
    }
    
    public static void generateJsonSchema() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        // configure mapper, if necessary, then create schema generator
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(mapper);
        JsonSchema schema = jsonSchemaGenerator.generateSchema(KeywordSourcePredicate.class);
        System.out.println(mapper.writeValueAsString(schema));
        
        System.out.println(KeywordPredicate.class.getSimpleName());
        
        Object obj = mapper.getSubtypeResolver().collectAndResolveSubtypesByClass(
                mapper.getDeserializationConfig(), 
                AnnotatedClass.construct(mapper.constructType(PredicateBase.class),
                        mapper.getDeserializationConfig()));
        
        System.out.println(obj);
        
    }

}
