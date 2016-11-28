package edu.uci.ics.textdb.sandbox.OpenNLPexample;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Scanner;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

public class POSTagexample {
	
    public static void main(String[] args) throws IOException {
    	
    	POSModel model = new POSModelLoader()	
    		.load(new File("./src/main/resources/en-pos-maxent.bin"));
    	PerformanceMonitor perfMon = new PerformanceMonitor(System.err, "sent");
    	POSTaggerME tagger = new POSTaggerME(model);
     
    	String dataFile = "./src/main/resources/abstract_100.txt";
    	Scanner scan = new Scanner(new File(dataFile));
    	
    	perfMon.start();
    	while(scan.hasNextLine()) {
    		String input = scan.nextLine();
    		ObjectStream<String> lineStream = new PlainTextByLineStream(
        			new StringReader(input));
    		
    		String line;
        	while ((line = lineStream.read()) != null) {
         
        		String whitespaceTokenizerLine[] = WhitespaceTokenizer.INSTANCE
        				.tokenize(line);
        		String[] tags = tagger.tag(whitespaceTokenizerLine);
         
        		POSSample sample = new POSSample(whitespaceTokenizerLine, tags);
        		System.out.println(sample.toString());
         
        		perfMon.incrementCounter();
        	}
    	}
    	
    	perfMon.stopAndPrintFinalResult();
    	scan.close();
    }
}
