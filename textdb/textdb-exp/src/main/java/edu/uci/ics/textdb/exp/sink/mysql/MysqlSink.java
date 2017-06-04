package edu.uci.ics.textdb.exp.sink.mysql;



import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;

public class MysqlSink implements ISink{
	private final MysqlSinkPredicate predicate;
    private IOperator inputOperator;
    private int cursor = CLOSED;
    private Schema outputSchema;
    private Connection connection;
    private Statement statement;
	
	public MysqlSink(MysqlSinkPredicate predicate){
		this.predicate = predicate;
	}

    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }

	@Override
	public Schema getOutputSchema() {
		return outputSchema;
	}

	@Override
	public void open() throws TextDBException {
		if(cursor == OPENED){
			return;
		}
		inputOperator.open();
		Schema inputSchema = inputOperator.getOutputSchema();
		outputSchema = new Schema(inputSchema.getAttributes().stream()
                .filter(attr -> ! attr.getAttributeName().equalsIgnoreCase(SchemaConstants._ID))
                .filter(attr -> ! attr.getAttributeName().equalsIgnoreCase(SchemaConstants.PAYLOAD))
                .filter(attr -> ! attr.getAttributeType().equals(AttributeType.LIST))
                .toArray(Attribute[]::new));
		// JDBC connection
		try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String url = "jdbc:mysql://"+predicate.getHost()+":"+predicate.getPort()+"/"+predicate.getDatabase()+"?autoReconnect=true&useSSL=true";
			this.connection = DriverManager.getConnection(url, predicate.getUsername(), predicate.getPassword());
			statement = connection.createStatement();
			mysqlDropTable();
			mysqlCreateTable();
			cursor = OPENED;
		}catch(Exception e){
			throw new DataFlowException("MysqlSink failed to connect to mysql database."+e.getMessage());
		}
	}

    @Override
    public Tuple getNextTuple() throws TextDBException {
        if (cursor == CLOSED) {
            return null;
        }        
        if (cursor >= predicate.getLimit() + predicate.getOffset()) {
            return null;
        }
        Tuple inputTuple = null;
        while (true) {
            inputTuple = inputOperator.getNextTuple();
            if (inputTuple == null) {
                return null;
            }
            cursor++;
            if (cursor > predicate.getOffset()) {
                break;
            }
        }
        Tuple resultTuple = Utils.removeFields(inputTuple, SchemaConstants._ID, SchemaConstants.PAYLOAD);    	
        mysqlInsertTuple(resultTuple);
        return resultTuple;
    }
	
    /**
     * Insert tuples into mysql database by calling this.getNextTuple. 
     * No output
     */
	@Override
	public void processTuples() throws TextDBException {
        Tuple tuple;
        while ((tuple = this.getNextTuple()) != null) {
        }
	}

	

	@Override
	public void close() throws TextDBException {
		if (cursor == CLOSED) {
            return;
        }
    	inputOperator.close();
        try {
			connection.close();
            cursor = CLOSED; 
		} catch (Exception e) {
		    throw new DataFlowException("MysqlSink fail to close. "+e.getMessage());
		}
	}
	
    /**
     * Collects ALL the tuples to an in-memory list
     * also insert all tuples into mysql database by calling this.getNextTuple
     * Based on testing, collectAllTuples is much slower than processingTuples on large input (1000 tuples)
     * @return a list of tuples
     * @throws TextDBException
     */
    public List<Tuple> collectAllTuples() throws TextDBException {
        ArrayList<Tuple> results = new ArrayList<>();
        Tuple tuple;
        while ((tuple = this.getNextTuple()) != null) {
            results.add(tuple);
        }
        return results;
    }
    
    public int mysqlDropTable(){
		String dropTableStatement = "DROP TABLE IF EXISTS "+ predicate.getTable() + ";";
		try {
			if(statement == null)
				statement = connection.createStatement();
			return statement.executeUpdate(dropTableStatement);
		} catch (SQLException e) {
			throw new DataFlowException("MysqlSink failed to drop table "+predicate.getTable()+". "+e.getMessage());
		}
    }
	
	/**
	 * Get the table schema from inputOperator, create table in mysql database
	 */
	private int mysqlCreateTable(){
		List<Attribute> attributeList = outputSchema.getAttributes();
		String createTableStatement = "CREATE TABLE " + predicate.getTable() +" (\n";
		for(int i = 0; i < attributeList.size(); i++){
			String curAttr = convertAttribute(attributeList.get(i));
			if(i == 0)
				createTableStatement += curAttr;
			else
				createTableStatement += ",\n"+curAttr;
		}
		createTableStatement += "\n); ";
		try {
			if(statement == null)
				statement = connection.createStatement();
			return statement.executeUpdate(createTableStatement);
		} catch (SQLException e) {
			throw new DataFlowException("MysqlSink failed to create table "+predicate.getTable()+". "+e.getMessage());
		}
	}
	
	/**
	 * 
	 * Convert a textDB attribute into one line of sql statement.
	 * TextDB attribute is from outputSchema.
	 * Used in the create table statement.
	 * @param attribute
	 * @return
	 */
	private String convertAttribute(Attribute attribute){
		String sqlAttrTypeName = attribute.getAttributeType().getName();
		String sqlStatement = "\t"+attribute.getAttributeName();
		switch(sqlAttrTypeName){
			case "integer": sqlStatement += " INT"; break;
			case "double": 	sqlStatement += " DOUBLE"; break;
			case "date": 	sqlStatement += " DATE"; break;
			default: 		sqlStatement += " TEXT"; break;	// Including string and text
		}
		return sqlStatement;
	}
	
	private int mysqlInsertTuple(Tuple tuple){
		String sqlStatemnt = "INSERT INTO "+predicate.getTable() +" VALUES(";
		List<IField> fieldList = new ArrayList<>();
    	for (int i = 0; i < outputSchema.getAttributeNames().size(); i++) {    	    
    		fieldList.add(tuple.getField(outputSchema.getAttributeNames().get(i)));
    	}
		for(int i = 0; i < fieldList.size(); i++){
			String curField = converField(fieldList.get(i));
			if(i == 0)
				sqlStatemnt += curField;
			else
				sqlStatemnt += ", "+curField;
		}
		sqlStatemnt += "); ";
		try {
			if(statement == null)
				statement = connection.createStatement();
			return statement.executeUpdate(sqlStatemnt);
		} catch (SQLException e) {
			throw new DataFlowException("MysqlSink failed to insert into table "+predicate.getTable()+". "+e.getMessage());
		}
	}
	/**
	 * TODO:: If the textDB STRING/TEXT field has "\'", it will create mysql syntax error.
	 * @param field
	 * @return
	 */
	private String converField(IField field){
		if(field == null){
			return "NULL";
		}
		else if(field instanceof DoubleField){
			return field.getValue().toString();
		}
		else if(field instanceof IntegerField){
			return field.getValue().toString();
		}
		else if(field instanceof DateField){ // Notice it's java.sql.Date not java.util.Date
			java.util.Date utilDate = (java.util.Date) field.getValue();
			return "\'"+(new java.sql.Date(utilDate.getTime())).toString() + "\'";
		}
		else{	// STRING, TEXT
			return "\'" + field.getValue().toString() +"\'";
		}
	}
	
}
