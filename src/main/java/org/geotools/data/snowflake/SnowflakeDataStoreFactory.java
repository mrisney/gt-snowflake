package org.geotools.data.snowflake;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.dbcp.BasicDataSource;
import org.geotools.api.data.Parameter;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeDataStoreFactory extends JDBCDataStoreFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeDataStoreFactory.class);

    /** parameter for database type */
    public static final Param DBTYPE =
            new Param(
                    "dbtype",
                    String.class,
                    "Type",
                    true,
                    "snowflake",
                    Collections.singletonMap(Parameter.LEVEL, "program"));

    public static final Param ACCOUNT =
            new Param("account", String.class, "Snowflake account", true);
    public static final Param SCHEMA = new Param("schema", String.class, "Schema", false);
    public static final Param JDBC_URL =
            new Param("connectionStr", String.class, "Connection JDBC URL", true);

    @Override
    protected String getDatabaseID() {
        return (String) DBTYPE.sample;
    }

    @Override
    public String getDisplayName() {
        return "Snowflake";
    }

    @Override
    public String getDescription() {
        return "Snowflake Database";
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
        parameters.remove(HOST.key);
        parameters.remove(PORT.key);
        parameters.put(ACCOUNT.key, ACCOUNT);
        parameters.put(USER.key, USER);
        parameters.put(PASSWD.key, PASSWD); // Use the default password parameter
        parameters.put(DATABASE.key, DATABASE);
        parameters.put(SCHEMA.key, SCHEMA);
        parameters.put(JDBC_URL.key, JDBC_URL);

        LOGGER.info("Setup parameters for Snowflake DataStore");
    }

    @Override
    protected String getJDBCUrl(Map<String, ?> params) throws IOException {
        String account = (String) ACCOUNT.lookUp(params);
        String database = (String) DATABASE.lookUp(params);
        String schema = (String) SCHEMA.lookUp(params);

        StringBuilder url = new StringBuilder();
        url.append("jdbc:snowflake://").append(account).append(".west-us-2.azure.snowflakecom");
        /*
                if (database != null && !database.isEmpty()) {
                    url.append("/").append(database);
                }

                // Append the schema as a query parameter if it's not null or empty
                if (schema != null && !schema.isEmpty()) {
                    if (database != null && !database.isEmpty()) {
                        url.append("?schema=").append(schema);
                    } else {
                        url.append("?db=").append(database).append("&schema=").append(schema);
                    }
                }
        */
        return url.toString();
    }

    @Override
    protected String getValidationQuery() {
        return "SELECT 1";
    }

    @Override
    public boolean canProcess(Map params) {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            return checkDBType(params) && super.canProcess(params);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Snowflake JDBC driver not found", e);
            return false;
        }
    }

    @Override
    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map<String, ?> params)
            throws IOException {
        SnowflakeSQLDialect dialect = (SnowflakeSQLDialect) dataStore.getSQLDialect();
        LOGGER.info("Creating Snowflake DataStore");
        logParameters(params);
        return dataStore;
    }

    private void logParameters(Map<String, ?> params) {
        LOGGER.info("Parameters set by the user:");
        for (Map.Entry<String, ?> entry : params.entrySet()) {
            LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
        }
    }

    @Override
    public BasicDataSource createDataSource(Map<String, ?> params) throws IOException {
        BasicDataSource dataSource = new BasicDataSource();
        String user = (String) USER.lookUp(params);
        String password = (String) PASSWD.lookUp(params);
        String account = (String) ACCOUNT.lookUp(params);
        String database = (String) DATABASE.lookUp(params);
        String schema = (String) SCHEMA.lookUp(params);

        String connectionJDBCUrl = (String) JDBC_URL.lookUp(params);

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

        String connectStr =
                "jdbc:snowflake://"
                        + account
                        + ".west-us-2.azure.snowflakecomputing.com?db="
                        + database;
        dataSource.setUrl(connectStr);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(getDriverClassName());
        dataSource.setConnectionProperties(properties.toString());

        LOGGER.info("Configured DataSource with Arrow disabled and custom properties");

        return dataSource;
    }
}
