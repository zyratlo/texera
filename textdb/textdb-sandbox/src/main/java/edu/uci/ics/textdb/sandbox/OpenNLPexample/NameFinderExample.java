package edu.uci.ics.textdb.sandbox.OpenNLPexample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.Span;

public class NameFinderExample {
   
    public static String[] Tokenize(String sentence) throws InvalidFormatException, IOException {
    	InputStream is = new FileInputStream("./src/main/resources/en-token.bin");
     
    	TokenizerModel model = new TokenizerModel(is);
     
    	Tokenizer tokenizer = new TokenizerME(model);
     
    	String tokens[] = tokenizer.tokenize(sentence);
     
    	is.close();
    	
    	return tokens;
    }

    
    public static void main(String[] args) throws IOException {
    	String dataFile = "./src/main/resources/abstract_100.txt";
    	Scanner scan = new Scanner(new File(dataFile));
    	
    	InputStream is = new FileInputStream("./src/main/resources/en-ner-location.bin");
     
    	TokenNameFinderModel model = new TokenNameFinderModel(is);
    	is.close();
     
    	NameFinderME nameFinder = new NameFinderME(model);
    	
    	PerformanceMonitor perfMon = new PerformanceMonitor(System.err, "sent");
    	perfMon.start();
    	while(scan.hasNextLine()) { 
    		
    		String[] sentence = Tokenize(scan.nextLine());
    		Span Spans[] = nameFinder.find(sentence);
    		perfMon.incrementCounter();
    		
    		if(Spans.length != 0) {
    			
    			for(String s: sentence) {
    				 
    				System.out.print("["+ s + "] ");
    			}
    			System.out.println("/n");
    		}
    		
    		for(Span s: Spans) {
    			System.out.println(s.toString());
    			for(int i = s.getStart(); i < s.getEnd(); i++)
    				System.out.println(sentence[i]);	
    		}	
    		
    		if(Spans.length != 0)
    			System.out.println();
    		
    	}
    	perfMon.stopAndPrintFinalResult();
    	scan.close();
    
    }
    

    
     
    
}
