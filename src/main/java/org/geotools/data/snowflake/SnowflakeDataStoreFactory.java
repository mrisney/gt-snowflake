package org.geotools.data.snowflake;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.dbcp.BasicDataSource;
import org.geotools.api.data.Parameter;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;
import org.geotools.util.logging.Logging;

public class SnowflakeDataStoreFactory extends JDBCDataStoreFactory {

	
	 private static final Logger LOGGER = Logging.getLogger(SnowflakeDataStoreFactory.class);


	/** parameter for database type */
	public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true, "snowflake",
			Collections.singletonMap(Parameter.LEVEL, "program"));

	public static final Param ACCOUNT = new Param("account", String.class, "Snowflake account", true);
	public static final Param SCHEMA = new Param("schema", String.class, "Schema", false);
	public static final Param CLOUD_PROVIDER = new Param("cloud provider", String.class, "Cloud Provider", true);
	public static final Param CLOUD_REGION = new Param("cloud region", String.class, "Cloud Region", true);
	//public static final Param JDBC_URL = new Param("connectionStr", String.class, "Connection JDBC URL", true);

	@Override
	protected String getDatabaseID() {
		LOGGER.log(Level.INFO, "getDatabaseID() -- " + (String) DBTYPE.sample);
		return (String) DBTYPE.sample;
	}

	@Override
	public String getDisplayName() {
		LOGGER.log(Level.INFO, "getDisplayName() -- Snowflake");
		return "Snowflake";
	}

	@Override
	public String getDescription() {
		LOGGER.log(Level.INFO, "getDescription() -- Snowflake Database");
		return "Snowflake Database";
	}

	@Override
	protected String getDriverClassName() {
		LOGGER.log(Level.INFO, "getDriverClassName() -- net.snowflake.client.jdbc.SnowflakeDriver");
		return "net.snowflake.client.jdbc.SnowflakeDriver";
	}

	@Override
	protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
		LOGGER.log(Level.INFO, "createSQLDialect()");
		return new SnowflakeSQLDialect(dataStore);
	}

	@Override
	protected void setupParameters(Map<String, Object> parameters) {
		super.setupParameters(parameters);
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

		LOGGER.log(Level.INFO, "Setup parameters for Snowflake DataStore\n\tAccount: " + ACCOUNT 
				+ "\n\tUser: " + USER
				+ "\n\tProvider: " + CLOUD_PROVIDER
				+ "\n\tRegion: " + CLOUD_REGION
				+ "\n\tPassword: " + PASSWD 
				+ "\n\tDatabase: " + DATABASE 
				+ "\n\tSchema: " + SCHEMA);
	}

	@Override
	protected String getJDBCUrl(Map<String, ?> params) throws IOException {
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
		
		LOGGER.log(Level.INFO, "getJDBCUrl() -- " + url.toString());
		return url.toString();
	}

	@Override
	protected String getValidationQuery() {
		LOGGER.log(Level.INFO, "validationQuery() -- SELECT 1");
		return "SELECT 1";
	}

	@Override
	public boolean canProcess(Map params) {
		try {
			Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
			LOGGER.log(Level.INFO, "canProcess() -- Found Class net.snowflake.client.jdbc.SnowflakeDriver ");
			return checkDBType(params) && super.canProcess(params);
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, "canProcess() -- Snowflake JDBC driver not found", e);
			return false;
		}
	}

	@Override
	protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map<String, ?> params) throws IOException {
		SnowflakeSQLDialect dialect = (SnowflakeSQLDialect) dataStore.getSQLDialect();
		LOGGER.log(Level.INFO, "Creating Snowflake DataStore");
		logParameters(params);
		return dataStore;
	}

	private void logParameters(Map<String, ?> params) {
		LOGGER.log(Level.INFO, "Parameters set by the user:");
		for (Map.Entry<String, ?> entry : params.entrySet()) {
			if (entry.getValue() == null)
			{
				LOGGER.log(Level.INFO, "\tKey: " + entry.getKey().toString() + " -- Value: NULL");
			}
			else
			{
				LOGGER.log(Level.INFO, "\tKey: " + entry.getKey().toString() + " -- Value: " + entry.getValue().toString());
			}
		}
	}

	@Override
	public BasicDataSource createDataSource(Map<String, ?> params) throws IOException {
		LOGGER.log(Level.INFO, "createDataSource() -- Start");
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

		LOGGER.log(Level.INFO, "Configured DataSource with Arrow disabled and custom properties");
		LOGGER.log(Level.INFO, "createDataSource() -- End");

		return dataSource;
	}
}
