package edu.uci.ics.textdb.exp.source.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

/**
 * FileSourceOperator reads a file or files under a directory and converts one file to one tuple.
 * 
 * The filePath in the predicate must be 1) a text file or 2) a directory
 * 
 * In case of a directory, FileSourceOperator supports recursively reading files 
 *   and specifying a max recursive depth.
 * 
 * The files must have one of the supported extensions: {@code supportedExtensions}
 * 
 * FileSourceOperator reads all content of one file and convert them to one tuple.
 *   The tuple will have one column, the attributeName as defined in {@code FileSourcePredicate},
 *   with the attributeType as TEXT.
 *   
 * In case of a directory, if the directory doesn't contain any file that 
 *   matches the allowed extensions, then an exception will be thrown.
 * 
 * @author Zuozhi Wang
 */
public class FileSourceOperator implements ISourceOperator {
    
    /*
     * A helper function that returns if the file's extension is supported.
     * The extensions are expected to NOT have the dot "." in the string.
     * For example, extensions may contain "txt", but not ".txt"
     */
    private static boolean isExtensionAllowed(List<String> allowedExtensions, Path path) {       
        return allowedExtensions.stream()
            .map(ext -> "."+ext)
            .filter(ext -> path.getFileName().toString().toLowerCase().endsWith(ext))
            .findAny().isPresent();
    }
    
    private final FileSourcePredicate predicate;
    // output schema of this file source operator
    private final Schema outputSchema;
    
    // a list of files, each of which is a valid text file
    private List<Path> pathList;
    
    // cursor indicating the current position
    private Integer cursor = CLOSED;

    
    public FileSourceOperator(FileSourcePredicate predicate) {
        this.predicate = predicate;
        this.outputSchema = new Schema(
                SchemaConstants._ID_ATTRIBUTE,
                new Attribute(predicate.getAttributeName(), AttributeType.TEXT));

        this.pathList = new ArrayList<>();
        
        Path filePath = Paths.get(predicate.getFilePath());
        if (! Files.exists(filePath)) {
            throw new RuntimeException(String.format("file %s doesn't exist", filePath));
        }
        
        if (Files.isDirectory(filePath)) {
            try {
                if (this.predicate.isRecursive()) {
                    pathList.addAll(Files.walk(filePath, this.predicate.getMaxDepth()).collect(Collectors.toList()));
                } else {
                    pathList.addAll(Files.list(filePath).collect(Collectors.toList()));
                }
                
            } catch (IOException e) {
                throw new RuntimeException(String.format(
                        "opening directory %s failed: " + e.getMessage(), filePath));
            }
        } else {
            pathList.add(filePath);
        }
                
        // filter directories, files starting with ".", 
        //   and files that don't end with allowedExtensions
        this.pathList = pathList.stream()
            .filter(path -> ! Files.isDirectory(path))
            .filter(path -> ! path.getFileName().startsWith("."))
            .filter(path -> isExtensionAllowed(this.predicate.getAllowedExtensions(), path))
            .collect(Collectors.toList());
        
        // check if the path list is empty
        if (pathList.isEmpty()) {
            // TODO: change it to TextDB RuntimeException
            throw new RuntimeException(String.format(
                    "the filePath: %s doesn't contain any valid text files. " + 
                    "File extension must be one of %s .", 
                    filePath, this.predicate.getAllowedExtensions()));
        } 
    }

    @Override
    public void open() throws TextDBException {
        if (cursor != CLOSED) {
            return;
        }
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED || cursor >= pathList.size()) {
            return null;
        }
        // keep iterating until 
        //   1) a file is converted to a tuple successfully
        //   2) the cursor reaches the end
        while (cursor < pathList.size()) {            
            try {
                String content = new String(
                        Files.readAllBytes(pathList.get(cursor)));
                // create a tuple according to the string
                // and assign a random ID to it
                Tuple tuple = new Tuple(
                        outputSchema, 
                        IDField.newRandomID(),
                        new TextField(content));
                cursor++;
                return tuple;
            } catch (IOException e) {
                // if reading the current path fails, increment the cursor and continue
                cursor++;
                continue;
            }
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
        return outputSchema;
    }
    
    public FileSourcePredicate getPredicate() {
        return this.predicate;
    }

}
