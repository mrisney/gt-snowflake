package org.geotools.data.snowflake;

import java.io.IOException;
import java.util.Collections;
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


	/** parameter for database type */
	public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true, "snowflake",
			Collections.singletonMap(Parameter.LEVEL, "program"));
	public static final Param DATABASE = new Param("database", String.class, "Database", true);
	public static final Param ACCOUNT = new Param("account", String.class, "Snowflake account", true);
	public static final Param SCHEMA = new Param("schema", String.class, "Schema", false);
	public static final Param CLOUD_PROVIDER = new Param("cloud provider", String.class, "Cloud Provider", true);
	public static final Param CLOUD_REGION = new Param("cloud region", String.class, "Cloud Region", true);
	//public static final Param JDBC_URL = new Param("connectionStr", String.class, "Connection JDBC URL", true);

	@Override
	protected String getDatabaseID() {
		LOGGER.entering(CLASS_NAME, "getDatabaseID");
		LOGGER.exiting(CLASS_NAME, "getDatabaseID", (String) DBTYPE.sample);
		return (String) DBTYPE.sample;
	}

	@Override
	public String getDisplayName() {
		LOGGER.entering(CLASS_NAME, "getDisplayName");
		LOGGER.exiting(CLASS_NAME, "getDisplayName", "Snowflake");
		return "Snowflake";
	}

	@Override
	public String getDescription() {
		LOGGER.entering(CLASS_NAME, "getDescription");
		LOGGER.exiting(CLASS_NAME, "getDescription", "Snowflake Database");
		return "Snowflake Database";
	}

	@Override
	protected String getDriverClassName() {
		LOGGER.entering(CLASS_NAME, "getDriverClassName");
		LOGGER.exiting(CLASS_NAME, "getDriverClassName", "net.snowflake.client.jdbc.SnowflakeDriver");
		return "net.snowflake.client.jdbc.SnowflakeDriver";
	}

	@Override
	protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
		LOGGER.entering(CLASS_NAME, "createSQLDialect", dataStore);
		LOGGER.exiting(CLASS_NAME, "createSQLDialect");
		return new SnowflakeDialectBasic(dataStore);
	}

	@Override
	protected void setupParameters(Map<String, Object> parameters) {
		super.setupParameters(parameters);
		
		LOGGER.entering(CLASS_NAME, "setupParameters", parameters);
		parameters.remove(HOST.key);
		parameters.remove(PORT.key);
		parameters.put(ACCOUNT.key, ACCOUNT);
		parameters.put(USER.key, USER);
		parameters.put(PASSWD.key, PASSWD); // Use the default password parameter
		parameters.put(DATABASE.key, DATABASE);
		parameters.put(SCHEMA.key, SCHEMA);
		parameters.put(CLOUD_PROVIDER.key, CLOUD_PROVIDER);
		parameters.put(CLOUD_REGION.key, CLOUD_REGION);
		//parameters.put(JDBC_URL.key, JDBC_URL);
		
		
		LOGGER.finer("Setup parameters for Snowflake DataStore\n\tAccount: " + ACCOUNT.toString()
				+ "\n\tUser: " + USER.toString()
				+ "\n\tProvider: " + CLOUD_PROVIDER.toString()
				+ "\n\tRegion: " + CLOUD_REGION.toString()
				+ "\n\tPassword: " + PASSWD.toString()
				+ "\n\tDatabase: " + DATABASE.toString() 
				+ "\n\tSchema: " + SCHEMA.toString());
		
		LOGGER.exiting(CLASS_NAME, "setupParameters", parameters);
	}

	@Override
	protected String getJDBCUrl(Map<String, ?> params) throws IOException {
		
		LOGGER.entering(CLASS_NAME, "getJDBCUrl", params);
		
		String account = (String) ACCOUNT.lookUp(params);
		String database = (String) DATABASE.lookUp(params);
		String schema = (String) SCHEMA.lookUp(params);
		String cloudProvider = (String) CLOUD_PROVIDER.lookUp(params);
		String cloudRegion = (String) CLOUD_REGION.lookUp(params);

		StringBuilder url = new StringBuilder();
		url.append("jdbc:snowflake://").append(account).append(".").append(cloudRegion).append(".").append(cloudProvider).append(".snowflakecomputing.com");
		/*
		 * if (database != null && !database.isEmpty()) {
		 * url.append("/").append(database); }
		 * 
		 * // Append the schema as a query parameter if it's not null or empty if
		 * (schema != null && !schema.isEmpty()) { if (database != null &&
		 * !database.isEmpty()) { url.append("?schema=").append(schema); } else {
		 * url.append("?db=").append(database).append("&schema=").append(schema); } }
		 */
		
		LOGGER.exiting(CLASS_NAME, "getJDBCUrl", url.toString());
		return url.toString();
	}

	@Override
	protected String getValidationQuery() {
		LOGGER.entering(CLASS_NAME, "getValidationQuery");
		LOGGER.exiting(CLASS_NAME, "getValidationQuery", "SELECT 1");
		return "SELECT 1";
	}

	@Override
	public boolean canProcess(Map params) {
		LOGGER.entering(CLASS_NAME, "canProcess", params);
		try {
			Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
			
			LOGGER.finer("Success -- Found Class net.snowflake.client.jdbc.SnowflakeDriver");
			LOGGER.exiting(CLASS_NAME, "canProcess", checkDBType(params) && super.canProcess(params));
			
			return checkDBType(params) && super.canProcess(params);
		} catch (ClassNotFoundException e) {
			
			LOGGER.finer("Failure -- Snowflake JDBC driver not found");
			LOGGER.exiting(CLASS_NAME, "canProcess", false);
			return false;
		}
	}

	@Override
	protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map<String, ?> params) throws IOException {
		//SnowflakeDialect dialect = (SnowflakeDialect) dataStore.getSQLDialect();
		LOGGER.entering(CLASS_NAME, "createDataStoreInternal", new Object[] {dataStore, params});
		logParameters(params);
		LOGGER.exiting(CLASS_NAME, "createDataStoreInternal", dataStore);
		return dataStore;
	}

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

	@Override
	public BasicDataSource createDataSource(Map<String, ?> params) throws IOException {
		
		LOGGER.entering(CLASS_NAME, "createDataSource", params);
		BasicDataSource dataSource = new BasicDataSource();
		String user = (String) USER.lookUp(params);
		String password = (String) PASSWD.lookUp(params);
		String account = (String) ACCOUNT.lookUp(params);
		String database = (String) DATABASE.lookUp(params);
		String schema = (String) SCHEMA.lookUp(params);
		String cloudProvider = (String) CLOUD_PROVIDER.lookUp(params);
		String cloudRegion = (String) CLOUD_REGION.lookUp(params);

		//String connectionJDBCUrl = (String) JDBC_URL.lookUp(params);

		Properties properties = new Properties();
		properties.put("user", user);
		properties.put("password", password);
		properties.put("account", account);
		properties.put("database", database);
		if (database != null) {
			properties.put("db", database);
		}
		if (schema != null) {
			properties.put("schema", schema);
		}
		properties.put("tracing", "all");
		properties.put("cloud_provider", cloudProvider);
		properties.put("cloud_region", cloudRegion);

		String connectStr = "jdbc:snowflake://" + account + "." + cloudRegion + "." + cloudProvider + ".snowflakecomputing.com?db=" + database;
		dataSource.setUrl(connectStr);
		dataSource.setUsername(user);
		dataSource.setPassword(password);
		dataSource.setDriverClassName(getDriverClassName());
		dataSource.setConnectionProperties(properties.toString());

		LOGGER.finer("Configured DataSource with Arrow disabled and custom properties");
		LOGGER.exiting(CLASS_NAME, "createDataSource", dataSource);

		return dataSource;
	}
}
