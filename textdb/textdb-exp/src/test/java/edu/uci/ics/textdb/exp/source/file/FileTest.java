package edu.uci.ics.textdb.exp.source.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class FileTest {
    
    private static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Path testPath = Paths.get("/Users/georgewang/Desktop/test/test1.txt");
        
        System.out.println(FileSourceOperator.isExtensionSupported(testPath));
        
//        Files.walk(testPath).sorted(Collections.reverseOrder()).peek(System.out::println).forEach(FileTest::delete);
    }

}
