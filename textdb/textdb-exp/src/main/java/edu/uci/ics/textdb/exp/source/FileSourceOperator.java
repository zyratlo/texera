package edu.uci.ics.textdb.exp.source;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
 * FileSourceOperator treats files on disk as a source. FileSourceOperator reads
 * a file line by line. A user needs to provide a custom function to convert a
 * string to tuple.
 * 
 * @author zuozhi
 */
public class FileSourceOperator implements ISourceOperator {
    
    // file must be a text file, and its extension must be one of the following
    public static final List<String> supportedExtensions = Arrays.asList(
            "txt", "json", "xml", "csv", "html");
    
    private final FileSourcePredicate predicate;
    // output schema of this file source operator
    private final Schema outputSchema;
    
    // a list of files, each is a valid text file
    private List<Path> pathList;
    
    // cursor indicating current position
    private Integer cursor = CLOSED;

    /**
     * FileSourceOperator reads a file or files under a directory and converts one file to one tuple.
     * 
     * The filePath in predicate must be 1) a text file or 2) a directory
     * In case of a directory, the files directly under this directory will be read.
     * The file must have one of the supported extensions: {@code supportedExtensions}
     * 
     * FileSourceOperator reads all content of one file and convert them to one tuple,
     *   the tuple will have one column, the attributeName is defined in {@code FileSourcePredicate},
     *   and the attributeType is TEXT
     * 
     * @param predicate, a {@code FileSourcePredicate}
     */
    public FileSourceOperator(FileSourcePredicate predicate) {
        this.predicate = predicate;
        this.outputSchema = new Schema(
                SchemaConstants._ID_ATTRIBUTE,
                new Attribute(predicate.getAttributeName(), AttributeType.TEXT));

        this.pathList = new ArrayList<>();
        
        Path filePath = Paths.get(predicate.getFilePath());
        if (Files.exists(filePath)) {
            throw new RuntimeException(String.format("file %s doesn't exist", filePath));
        }
        
        if (Files.isDirectory(filePath)) {
            try {
                pathList.addAll(Files.list(filePath).collect(Collectors.toList()));
            } catch (IOException e) {
                throw new RuntimeException(String.format(
                        "opening directory %s failed: " + e.getMessage(), filePath));
            }
        } else {
            pathList.add(filePath);
        }
        
        // filter directories, files that start with ".", 
        // and files that don't end with supportedExtensions
        this.pathList = pathList.stream()
            .filter(path -> Files.isDirectory(path))
            .filter(path -> path.getFileName().startsWith("."))
            .filter(path -> supportedExtensions.stream().map(ext -> "."+ext)
                    .filter(ext -> path.getFileName().endsWith(ext)).findAny().isPresent())
            .collect(Collectors.toList());
        
        // check if path list is empty
        if (pathList.isEmpty()) {
            // TODO: change it to TextDB RuntimeException
            throw new RuntimeException(String.format(
                    "the filePath: %s doesn't contain any valid text files. " + 
                    "File extension must be one of %s .", 
                    filePath, supportedExtensions));
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
        //   2) cursor reaches the end
        while (cursor < pathList.size()) {            
            try {
                String content = new String(
                        Files.readAllBytes(pathList.get(cursor)), Charset.defaultCharset());
                // create a tuple according to the string
                // and assign a random ID to it
                Tuple tuple = new Tuple(
                        outputSchema, 
                        IDField.newRandomID(),
                        new TextField(content));
                cursor++;
                return tuple;
            } catch (IOException e) {
                // if reading current path fails, increment cursor and continue
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
