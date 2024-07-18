package org.geotools.data.snowflake;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.dbcp.BasicDataSource;
import org.geotools.api.data.Parameter;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;
import org.geotools.util.logging.Logging;

public class SnowflakeDataStoreFactory extends JDBCDataStoreFactory {

	
	 private static final Logger LOGGER = Logging.getLogger(SnowflakeDataStoreFactory.class);
	 private static final String CLASS_NAME = "SnowflakeDataStoreFactory";
	 
//	 public static Map<String, Object> cloudOptions;
//	 static {
//		 cloudOptions = new HashMap<>();
//		 
//		 cloudOptions.put(Param.OPTIONS, CloudOptions.getCloudOptions());
//	 }


	/** parameter for database type */
	public static final Param DBTYPE = new Param("DB Type", String.class, "Type", true, "Snowflake",
			Collections.singletonMap(Parameter.LEVEL, "program"));
	public static final Param DATABASE = new Param("Database", String.class, "Database", true);
	public static final Param ACCOUNT = new Param("Account Identifier", String.class, "Snowflake account identifier", true);
	public static final Param SCHEMA = new Param("Schema", String.class, "Schema", false);
	public static final Param CLOUD_SELECTION = new Param("Cloud Selection", String.class, "Cloud provider and region selection", true, "AWS : us-west-2", CloudOptions.getCloudMetadata());
	
	public static final String SNOWFLAKE_DRIVER_CLASS_NAME = "net.snowflake.client.jdbc.SnowflakeDriver";
	
	// Returns the sample data from the DBTYPE param
	@Override
	protected String getDatabaseID() {
		return (String) DBTYPE.sample;
	}

	// Returns Snowflake since this is a Snowflake Datastore
	@Override
	public String getDisplayName() {
		return "Snowflake";
	}

	// Returns some description of the Snowflake database
	@Override
	public String getDescription() {
		return "Snowflake Database";
	}

	// Returns the Driver Class Name for the Snowflake JDBC Driver
	@Override
	protected String getDriverClassName() {
		return SNOWFLAKE_DRIVER_CLASS_NAME;
	}

	// Returns an instance of the Dialect class to use with the Snowflake Datastore
	@Override
	protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
		return new SnowflakeDialectBasic(dataStore);
	}

	// Create the map of parameters used to connect to Snowflake
	@Override
	protected void setupParameters(Map<String, Object> parameters) {
		super.setupParameters(parameters);
		
		// Host and port aren't required for a Snowflake connection so we can remove them
		parameters.remove(HOST.key);
		parameters.remove(PORT.key);
		
		// Required params for connecting to Snowflake
		parameters.put(ACCOUNT.key, ACCOUNT);
		parameters.put(USER.key, USER);
		parameters.put(PASSWD.key, PASSWD);
		parameters.put(CLOUD_SELECTION.key, CLOUD_SELECTION);
		
		// Optional params for connecting to Snowflake
		parameters.put(DATABASE.key, DATABASE);
		parameters.put(SCHEMA.key, SCHEMA);
	}

	// Constructs the JDBCUrl based on parameters input by the user in the GeoServer UI
	@Override
	protected String getJDBCUrl(Map<String, ?> params) throws IOException {
		
		// Required params for connecting to Snowflake
		String account = (String) ACCOUNT.lookUp(params);
		String cloudSelection = (String) CLOUD_SELECTION.lookUp(params);
		
		// Optional params for connecting to Snowflake
		String database = (String) DATABASE.lookUp(params);
		String schema = (String) SCHEMA.lookUp(params);
		
		// Parse out the cloud provider and cloud region from the cloud selection dropdown
		String cloudProvider = cloudSelection.split(" ")[0].toLowerCase();
		String cloudRegion = cloudSelection.split(" ")[2];

		StringBuilder url = new StringBuilder();
		// Start constructing the URL based on the required fields 
		url.append("jdbc:snowflake://").append(account).append(".").append(cloudRegion).append(".").append(cloudProvider).append(".snowflakecomputing.com/");
		
		// Append the database name as a query parameter if one was provided
		if (database != null && !database.isBlank()) {
			url.append("?db=").append(database); 
		}
		
		// Append the schema as a query parameter if one was provided
		if (schema != null && !schema.isBlank()) { 	
			url.append("&schema=").append(schema); 
		}
		 
		return url.toString();
	}

	// Simple validation query ran once the connection is successful
	@Override
	protected String getValidationQuery() {
		return "SELECT 1";
	}

	
	// Checks whether the connector can access the Snowflake JDBC Driver class
	@Override
	public boolean canProcess(Map<String, ?> params) {
		
		try {
			Class.forName(SNOWFLAKE_DRIVER_CLASS_NAME);
			return checkDBType(params) && super.canProcess(params);
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	// Creates the internal Datastore
	@Override
	protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map<String, ?> params) throws IOException {
		logParameters(params);
		return dataStore;
	}

	// Helper function for logging parameters input by the user in the GeoServer UI
	private void logParameters(Map<String, ?> params) {
		
		LOGGER.finer("Parameters set by the user:");
		for (Map.Entry<String, ?> entry : params.entrySet()) {
			if (entry.getValue() == null)
			{	
				LOGGER.finer("\tKey: " + entry.getKey().toString() + " -- Value: NULL");
			}
			else
			{
				LOGGER.finer("\tKey: " + entry.getKey().toString() + " -- Value: " + entry.getValue().toString());
			}
		}
	}

	// Creates a BasicDataSource using the parameters input by the user in the GeoServer UI
	@Override
	public BasicDataSource createDataSource(Map<String, ?> params) throws IOException {
		
		BasicDataSource dataSource = new BasicDataSource();
		
		// Required params for connecting to Snowflake
		String user = (String) USER.lookUp(params);
		String password = (String) PASSWD.lookUp(params);
		String account = (String) ACCOUNT.lookUp(params);
		String cloudSelection = (String) CLOUD_SELECTION.lookUp(params);
		
		// Optional params for connecting to Snowflake
		String database = (String) DATABASE.lookUp(params);
		String schema = (String) SCHEMA.lookUp(params);
		
		// Parse the cloud provider and cloud region from the Cloud Selection dropdown
		String cloudProvider = cloudSelection.split(" ")[0].toLowerCase();
		String cloudRegion = cloudSelection.split(" ")[2];

		// Create a mapping of properties
		Properties properties = new Properties();
		properties.put("user", user);
		properties.put("password", password);
		properties.put("account", account);
		properties.put("database", database);
		String connectStr = "jdbc:snowflake://" + account + "." + cloudRegion + "." + cloudProvider + ".snowflakecomputing.com/";
		
		// Append the database parameter if one was provided
		if (database != null && !database.isBlank()) {
			properties.put("db", database);
			connectStr += "?db=" + database;
		}
		
		// Append the schema parameter if one was provided
		if (schema != null && !schema.isBlank()) {
			properties.put("schema", schema);
			connectStr += "&schema=" + schema;
		}
		properties.put("tracing", "all");

		// Finish configuring the datastore
		dataSource.setUrl(connectStr);
		dataSource.setUsername(user);
		dataSource.setPassword(password);
		dataSource.setDriverClassName(getDriverClassName());
		dataSource.setConnectionProperties(properties.toString());

		return dataSource;
	}
}
