package edu.uci.ics.texera.api.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.uci.ics.texera.api.constants.DataConstants;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

public class Utils {
	
	public static Path getDefaultIndexDirectory() throws StorageException {
		return getTexeraHomePath().resolve("index");
	}
    
    /**
     * Gets the path of resource files under the a subproject's resource folder (in src/main/resources)
     * 
     * @param resourcePath, the path to a resource relative to subproject/src/main/resources
     * @param subProject, the sub project where the resource is located
     * @return the path to the resource
     * @throws StorageException if finding fails
     */
    public static Path getResourcePath(String resourcePath, TexeraProject subProject) throws StorageException {
        return getTexeraHomePath()
        			.resolve(subProject.getProjectName())
        			.resolve("src/main/resources")
        			.resolve(resourcePath);
    }
    
    /**
     * Gets the real path of the texera home directory by:
     *   1): try to use TEXERA_HOME environment variable, 
     *   if it fails then:
     *   2): compare if the current directory is texera (where TEXERA_HOME should be), 
     *   if it's not then:
     *   3): compare if the current directory is a texera subproject, 
     *   if it's not then:
     *   
     *   Finding texera home directory will fail
     * 
     * @return the real absolute path to texera home directory
     * @throws StorageException if can not find texera home
     */
    public static Path getTexeraHomePath() throws StorageException {
        try {
            // try to use TEXERA_HOME environment variable first
            if (System.getenv(DataConstants.HOME_ENV_VAR) != null) {
                String texeraHome = System.getenv(DataConstants.HOME_ENV_VAR);
                return Paths.get(texeraHome).toRealPath();
            } else {
                // if the environment variable is not found, try if the current directory is texera
                Path currentWorkingDirectory = Paths.get(".").toRealPath();
                
                // if the current directory is "texera/core" (TEXERA_HOME location)
                boolean isTexeraHome = currentWorkingDirectory.endsWith("core")
                		&& currentWorkingDirectory.getParent().endsWith("texera");
                if (isTexeraHome) {
                    return currentWorkingDirectory;
                }
                
                // if the current directory is one of the sub-projects
                boolean isSubProject = Arrays.asList(TexeraProject.values()).stream()
                    .map(project -> project.getProjectName())
                    .filter(project -> currentWorkingDirectory.endsWith(project)).findAny().isPresent();
                if (isSubProject) {
                    return currentWorkingDirectory.getParent().toRealPath();
                }
                
                throw new StorageException(
                        "Finding texera home path failed. Current working directory is " + currentWorkingDirectory);
            }
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

   /**
    * Removes one or more attributes from the schema and returns the new schema.
    * 
    * @param schema
    * @param attributeName
    * @return
    */
   public static Schema removeAttributeFromSchema(Schema schema, String... attributeName) {
       return new Schema(schema.getAttributes().stream()
               .filter(attr -> (! Arrays.asList(attributeName).contains(attr.getName())))
               .toArray(Attribute[]::new));
   }
   
   /**
    * Creates a new schema object, with "_ID" attribute added to the front.
    * If the schema already contains "_ID" attribute, returns the original schema.
    * 
    * @param schema
    * @return
    */
   public static Schema getSchemaWithID(Schema schema) {
       if (schema.containsAttribute(SchemaConstants._ID_ATTRIBUTE.getName())) {
           return schema;
       }
       return new Schema.Builder().add(SchemaConstants._ID_ATTRIBUTE).add(schema).build();    
   }
   
   /**
    * Remove one or more fields from each tuple in tupleList.
    * 
    * @param tupleList
    * @param removeFields
    * @return
    */
   public static List<Tuple> removeFields(List<Tuple> tupleList, String... removeFields) {
       List<Tuple> newTuples = tupleList.stream().map(tuple -> removeFields(tuple, removeFields))
               .collect(Collectors.toList());
       return newTuples;
   }
   
   /**
    * Remove one or more fields from a tuple.
    * 
    * @param tuple
    * @param removeFields
    * @return
    */
   public static Tuple removeFields(Tuple tuple, String... removeFields) {
       List<String> removeFieldList = Arrays.asList(removeFields);
       List<Integer> removedFieldsIndex = removeFieldList.stream()
               .map(attributeName -> tuple.getSchema().getIndex(attributeName)).collect(Collectors.toList());
       
       Attribute[] newAttrs = tuple.getSchema().getAttributes().stream()
               .filter(attr -> (! removeFieldList.contains(attr.getName()))).toArray(Attribute[]::new);
       Schema newSchema = new Schema(newAttrs);
       
       IField[] newFields = IntStream.range(0, tuple.getSchema().getAttributes().size())
           .filter(index -> (! removedFieldsIndex.contains(index)))
           .mapToObj(index -> tuple.getFields().get(index)).toArray(IField[]::new);
       
       return new Tuple(newSchema, newFields);
   }

}
