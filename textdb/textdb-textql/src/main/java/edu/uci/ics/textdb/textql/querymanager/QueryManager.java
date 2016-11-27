package edu.uci.ics.textdb.textql.querymanager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import edu.uci.ics.textdb.plangen.LogicalPlan;

/**
 * Wraps the parser for TextQL generated with JavaCC. This class allows easy interface with with parse and get  
 * 
 * @author Flavio Bayer
 */
public class QueryManager {
	//InputStream used as the source for the underlying parser
	private InputStream input;
	//OutpuStream used as by the underlying parser to print out results of each parsed statement
	private OutputStream output;
	//parsedStatements is a List of statements that have been parsed from the input
	//each statement is a pair <key,value> for the parameter of the statement
	//each statement has at least the keys "statementType" and "statementName"(identifier)
	private List<Map<String, Object>> parsedStatements;
	private Map<String, Map<String, Object>> nameVsStatement;
	
	public QueryManager() {
		input = null;
		output = null;
		parsedStatements = null;
		nameVsStatement = null;
	}
	
	/**
	 * Set the InputStream used during the parsing 
	 * @param inputStream  the new InputStream
	 */
	void setInput(InputStream inputStream){
		input = inputStream;
	}
	
	/**
	 * Create a new InputStream from the file f to be used during the parsing
	 * The content of the file will be used as the whole input
	 * @param f  the File used as input InputStream
	 */
	void setInput(File f) throws FileNotFoundException{
		input = new FileInputStream(f);
	}

	/**
	 * Create a new InputStream from the string s to be used during the parsing 
	 * The content of the string s will be used as the whole input
	 * @param inputStream  the new InputStream
	 */
	void setInput(String s) throws IOException{
		PipedOutputStream pos = new PipedOutputStream();
		PipedInputStream pis = new PipedInputStream(pos);
		PrintStream ppos = new PrintStream(pos);
		ppos.print(s);
		input = pis;
	}
	
	/**
	 * Create a new InputStream from the string s to be used during the parsing 
	 * The content of the string s will be used as the whole input
	 * @param inputStream  the new InputStream
	 */
	void setOutput(OutputStream outputStream) throws IOException{
		output = outputStream;
	}
	
	/**
	 * Parse the current input and generate internal structures required
	 * to export views using getView or getOutputView.
	 * @throws IOException If an exception occurs while closing Input or Output Streams
	 */
	void parse() throws IOException{
		input.close();
		output.close();
	}
	
	/**
	 * Generates a logical plan for the view defined as output
	 * that is, the view that was used in the "OUTPUT VIEW" statement
	 * @return The generated LogicalPlan for the view
	 */
	LogicalPlan getOutputView(){
		String[] outputViews = parsedStatements.stream()
				.filter(s->s.get("statementType").equals("output"))
				.map(s->s.get("viewName"))
				.toArray(String[]::new);
		if(outputViews.length!=1)return null;
		return getView(outputViews[0]);
	}

	/**
	 * Generates a logical plan for the view with specific name.
	 * @param viewName	Name of the view to be selected
	 * @return The generated LogicalPlan for the view
	 */
	LogicalPlan getView(String viewName){		 
		return null;
	}
	
}
