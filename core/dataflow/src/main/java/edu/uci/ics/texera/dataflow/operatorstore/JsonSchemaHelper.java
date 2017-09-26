package edu.uci.ics.texera.dataflow.operatorstore;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;

import edu.uci.ics.texera.api.constants.DataConstants;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.plangen.OperatorArityConstants;

@SuppressWarnings("unchecked")
public class JsonSchemaHelper {
    
    private static ObjectMapper objectMapper = DataConstants.defaultObjectMapper;
    
    public static HashMap<Class<? extends PredicateBase>, String> operatorTypeMap = new HashMap<>();
    static {
        // find all the operator type declarations in PredicateBase annotation
        Collection<NamedType> types = objectMapper.getSubtypeResolver().collectAndResolveSubtypesByClass(
                objectMapper.getDeserializationConfig(), 
                AnnotatedClass.construct(objectMapper.constructType(PredicateBase.class),
                        objectMapper.getDeserializationConfig()));
        
        for (NamedType type : types) {
            if (type.getType() != null && type.getName() != null) {
                operatorTypeMap.put((Class<? extends PredicateBase>) type.getType(), type.getName());
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        generateAllOperatorSchema();
    }
    
    public static void generateAllOperatorSchema() throws Exception {
        for (Class<? extends PredicateBase> predicateClass : operatorTypeMap.keySet()) {
            generateJsonSchema(predicateClass);
        }
    }
    
    public static void generateJsonSchema(Class<? extends PredicateBase> predicateClass) throws Exception {

        // find the operatorType of the predicate class
        String operatorType = operatorTypeMap.get(predicateClass);
        
        // find the operator json schema path by its predicate class
        Path operatorSchemaPath = getJsonSchemaPath(predicateClass);
        
        // create the json schema file of the operator
        Files.deleteIfExists(operatorSchemaPath);
        Files.createFile(operatorSchemaPath);   
        
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(DataConstants.defaultObjectMapper);
        JsonSchema schema = jsonSchemaGenerator.generateSchema(predicateClass);
        
        ObjectNode schemaNode = objectMapper.readValue(objectMapper.writeValueAsBytes(schema), ObjectNode.class);
        // remove the operatorID from the json schema
        ((ObjectNode) schemaNode.get("properties")).remove("operatorID");
        // add the operator type to the schema
        schemaNode.put("operatorType", operatorType);
        // add input and output arity to the schema
        schemaNode.put("inputNumber", OperatorArityConstants.getFixedInputArity(predicateClass));
        schemaNode.put("outputNumber", OperatorArityConstants.getFixedOutputArity(predicateClass));
        
        // add additional operator metadata to the schema
        Map<String, Object> operatorMetadata = (Map<String, Object>) predicateClass.getMethod("getOperatorMetadata").invoke(null);
        for (String key : operatorMetadata.keySet()) {
            schemaNode.set(key, objectMapper.valueToTree(operatorMetadata.get(key)));
        }
        
        // add required/optional properties to the schema
        List<String> requriedProperties = getRequiredProperties(predicateClass);
        schemaNode.set("required", objectMapper.valueToTree(requriedProperties));
        
        // add advancedOption properties to the schema
        List<String> advancedOptionProperties = getAdvancedOptionProperties(predicateClass);
        schemaNode.set("advancedOptions", objectMapper.valueToTree(advancedOptionProperties));
        
        Files.write(operatorSchemaPath, objectMapper.writeValueAsBytes(schemaNode));
        
        System.out.println("generating schema of " + operatorType + " completed");
    }
    
    public static Path getJsonSchemaPath(Class<? extends PredicateBase> predicateClass) {
        // find the operatorType of the predicate class
        String operatorType = operatorTypeMap.get(predicateClass);
        
        // find the operator directory by its predicate class
        Path classDirectory = Utils.getTexeraHomePath()
            .resolve(DataConstants.TexeraProject.TEXERA_DATAFLOW.getProjectName())
            .resolve("src/main/java")
            .resolve(predicateClass.getPackage().getName().replace('.', '/'));
        
        Path operatorSchemaPath = classDirectory.resolve(operatorType + "Schema.json");
        
        return operatorSchemaPath;
    }
    
    public static List<String> getRequiredProperties(Class<? extends PredicateBase> predicateClass) {
        ArrayList<String> requiredProperties = new ArrayList<>();
        
        // get all constructors of the class
        Constructor<?>[] constructors = predicateClass.getConstructors();
        for (Constructor<?> constructor : constructors) {
            // find the constructor with @JsonCreator annotation
            JsonCreator jsonCreatorAnnotation = constructor.getAnnotation(JsonCreator.class);
            if (jsonCreatorAnnotation == null) {
                continue;
            }
            // find the @JsonProperty annotation for each parameter
            for (Annotation[] annotations : Arrays.asList(constructor.getParameterAnnotations())) {
                for (Annotation annotation : Arrays.asList(annotations)) {
                    if (! annotation.annotationType().equals(JsonProperty.class)) {
                        continue;
                    }
                    JsonProperty jsonProperty = (JsonProperty) annotation;
                    if (jsonProperty.required()) {
                        requiredProperties.add(jsonProperty.value());
                    }
                }
            }
        }
        return requiredProperties;
    }
    
    public static List<String> getAdvancedOptionProperties(Class<? extends PredicateBase> predicateClass) {
        ArrayList<String> advancedProperties = new ArrayList<>();
        
        // get all constructors of the class
        Constructor<?>[] constructors = predicateClass.getConstructors();
        for (Constructor<?> constructor : constructors) {
            // find the constructor with @JsonCreator annotation
            JsonCreator jsonCreatorAnnotation = constructor.getAnnotation(JsonCreator.class);
            if (jsonCreatorAnnotation == null) {
                continue;
            }
            // find the @AdvancedOption annotation for each parameter
            for (Annotation[] annotations : Arrays.asList(constructor.getParameterAnnotations())) {
                Optional<Annotation> findAdvancedOptionAnnotation = Arrays.asList(annotations).stream()
                        .filter(annotation -> annotation.annotationType().equals(AdvancedOption.class)).findAny();
                if (! findAdvancedOptionAnnotation.isPresent()) {
                    continue;
                }
                AdvancedOption advancedOptionAnnotation = (AdvancedOption) findAdvancedOptionAnnotation.get();
                
                Optional<Annotation> findJsonProperty = Arrays.asList(annotations).stream()
                        .filter(annotation -> annotation.annotationType().equals(JsonProperty.class)).findAny();
                if (! findJsonProperty.isPresent()) {
                    continue;
                }
                JsonProperty jsonProperty = (JsonProperty) findJsonProperty.get();
                
                if (advancedOptionAnnotation.isAdvancedOption() == true) {
                    advancedProperties.add(jsonProperty.value());

                }
            }
        }
        
        return advancedProperties;
    }
    
    public static void getPropertyDefaultValues(Class<? extends PredicateBase> predicateClass) {
        
    }

}
