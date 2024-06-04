package org.geotools.data.snowflake;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.SQLDialect;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;

public class SnowflakeDataStore {

    private JDBCDataStore dataStore;

    public SnowflakeDataStore(Map<String, Object> params) throws IOException {
        try {
            DataSource dataSource = createDataSource(params);
            dataStore = new JDBCDataStore();
            dataStore.setDataSource(dataSource);
            dataStore.setSQLDialect(createSQLDialect());
            dataStore.setNamespaceURI((String) params.get("namespace"));
        } catch (SQLException e) {
            throw new IOException("Failed to create a Snowflake connection", e);
        }
    }

    private SQLDialect createSQLDialect() {
        return new SnowflakeSQLDialect(dataStore);
    }

    private DataSource createDataSource(Map<String, Object> params) throws SQLException {
        String user = (String) params.get(SnowflakeDataStoreFactory.USER.key);
        String password = (String) params.get(SnowflakeDataStoreFactory.PASSWORD.key);
        String account = (String) params.get(SnowflakeDataStoreFactory.ACCOUNT.key);
        String database = (String) params.get(SnowflakeDataStoreFactory.DATABASE.key);
        String schema = (String) params.get(SnowflakeDataStoreFactory.SCHEMA.key);

        Properties properties = new Properties();
        properties.put("user", user);
        properties.put("password", password);
        properties.put("account", account);
        if (database != null) {
            properties.put("db", database);
        }
        if (schema != null) {
            properties.put("schema", schema);
        }

        String connectStr = "jdbc:snowflake://" + account + ".snowflakecomputing.com";
        Connection connection = DriverManager.getConnection(connectStr, properties);
        return new SimpleDataSource(connection);
    }

    public JDBCDataStore getDataStore() {
        return dataStore;
    }
}
