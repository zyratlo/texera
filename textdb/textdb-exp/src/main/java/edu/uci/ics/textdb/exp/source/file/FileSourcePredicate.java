package edu.uci.ics.textdb.exp.source.file;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.exp.common.PropertyNameConstants;

public class FileSourcePredicate {
    
    private final String filePath;
    private final String attributeName;
    private final Integer maxDepth;
    private final Boolean recursive;
    
    /**
     * FileSourcePredicate is used by FileSource Operator.
     * 
     * 
     * @param filePath, the path to a file or a directory
     * @param attributeName, the name of the attribute that the file content will be put in
     * @param recursive, if recursively list files or not (in the case of a directory), optional, default False
     * @param maxDepth, specify the max recursive depth (if recursive is True), optional, default Integer.MAX_VALUE
     */
    public FileSourcePredicate(
            @JsonProperty(value = PropertyNameConstants.FILE_PATH, required = true)
            String filePath, 
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attributeName,
            @JsonProperty(value = PropertyNameConstants.FILE_RECURSIVE, required = false)
            Boolean recursive,
            @JsonProperty(value = PropertyNameConstants.FILE_MAX_DEPTH, required = false)
            Integer maxDepth) {
        this.filePath = filePath;
        this.attributeName = attributeName;
        
        if (recursive == null) {
            this.recursive = false;
        } else {
            this.recursive = recursive;
        }
        if (maxDepth == null) {
            this.maxDepth = Integer.MAX_VALUE;
        } else {
            this.maxDepth = maxDepth;
        }  
    }
    
    @JsonProperty(PropertyNameConstants.FILE_PATH)
    public String getFilePath() {
        return this.filePath;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttributeName() {
        return this.attributeName;
    }
    
    @JsonProperty(PropertyNameConstants.FILE_RECURSIVE)
    public Boolean isRecursive() {
        return this.recursive;
    }
    
    @JsonProperty(PropertyNameConstants.FILE_MAX_DEPTH)
    public Integer getMaxDepth() {
        return this.maxDepth;
    }

}
