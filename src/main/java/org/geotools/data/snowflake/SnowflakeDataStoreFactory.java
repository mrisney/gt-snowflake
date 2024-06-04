package org.geotools.data.snowflake;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;
import org.geotools.data.Parameter;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SnowflakeDataStoreFactory extends JDBCDataStoreFactory {

	/** parameter for database type */
	public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true, "snowflake",
			Collections.singletonMap(Parameter.LEVEL, "program"));

	public static final Param ACCOUNT = new Param("account", String.class, "Snowflake account", true);
	public static final Param USER = new Param("user", String.class, "Username", true);
	public static final Param PASSWORD = new Param("password", String.class, "Password", true);
	public static final Param DATABASE = new Param("db", String.class, "Database", false);
	public static final Param SCHEMA = new Param("schema", String.class, "Schema", false);

	@Override
	public String getDisplayName() {
		return "Snowflake";
	}

	@Override
	public String getDescription() {
		return "Snowflake Database";
	}

	@Override
	protected String getDatabaseID() {
		return "snowflake";
	}

	@Override
	protected String getDriverClassName() {
		return "net.snowflake.client.jdbc.SnowflakeDriver";
	}

	@Override
	protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
		return new SnowflakeSQLDialect(dataStore);
	}


	@Override
	protected void setupParameters(Map<String, Object> parameters) {
		super.setupParameters(parameters);
		parameters.put(DBTYPE.key, DBTYPE);
		parameters.put(ACCOUNT.key, ACCOUNT);
		parameters.put(USER.key, USER);
		parameters.put(PASSWORD.key, PASSWORD);
		parameters.put(DATABASE.key, DATABASE);
		parameters.put(SCHEMA.key, SCHEMA);
	}

	@Override
	protected String getValidationQuery() {
		return "SELECT 1";
	}

	private JDBCDataStore createSnowflakeDataStore(JDBCDataStore dataStore, Map<String, Object> params)
			throws IOException {
		try {
			SnowflakeDataStore snowflakeDataStore = new SnowflakeDataStore(params);
			return snowflakeDataStore.getDataStore();
		} catch (Exception e) {
			throw new IOException("Failed to create SnowflakeDataStore", e);
		}
	}

	@Override
	public boolean canProcess(Map params) {
		try {
			Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
			return checkDBType(params) && super.canProcess(params);
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		return Collections.emptyMap();
	}

}
