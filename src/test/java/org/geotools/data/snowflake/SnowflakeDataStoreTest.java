package org.geotools.data.snowflake;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SnowflakeDataStoreTest {

    private Connection connection;

    @Before
    public void setUp() throws Exception {
        connection = getConnection();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void testConnectionAndQuery() {
        try {
            // Create statement
            Statement statement = connection.createStatement();

            // Create a table
            String query = "CREATE OR REPLACE TABLE demo(C1 STRING)";
            statement.executeUpdate(query);

            // Verify the table was created
            String verifyQuery = "SHOW TABLES LIKE 'DEMO'";
            assertTrue(statement.execute(verifyQuery));

            // Drop the table
            statement.executeUpdate("DROP TABLE demo");

            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to execute query: " + e.getLocalizedMessage());
        }
    }

    private Connection getConnection() throws Exception {
        Properties properties = new Properties();
        
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("snowflake.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find snowflake.properties");
            }
            properties.load(input);
        }

        // Debug output to verify properties
        System.out.println("Properties loaded: " + properties);

        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

        // Extract properties and check for null values
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        String account = properties.getProperty("account");
        String db = properties.getProperty("db");
        String schema = properties.getProperty("schema");
        String connectStr = properties.getProperty("connectStr");

        if (user == null || password == null || account == null || db == null || schema == null || connectStr == null) {
            throw new RuntimeException("Missing required connection properties");
        }

        properties.put("user", user);
        properties.put("password", password);
        properties.put("account", account);
        properties.put("db", db);
        properties.put("schema", schema);

        // Set login timeout
        DriverManager.setLoginTimeout(10); // 10 seconds
        
        System.out.println("Connecting to Snowflake with: " + connectStr);
        return DriverManager.getConnection(connectStr, properties);
    }
}
