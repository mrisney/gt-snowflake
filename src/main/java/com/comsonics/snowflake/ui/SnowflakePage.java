package com.comsonics.snowflake.ui;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.geoserver.web.GeoServerBasePage;

@SuppressWarnings("serial")
public class SnowflakePage extends GeoServerBasePage {

    public SnowflakePage() {
        add(new FeedbackPanel("feedback"));
        SnowflakeParamPanel paramPanel = new SnowflakeParamPanel("paramPanel");
        Form form =
                new Form("form") {
                    protected void onSubmit() {

                        try {
                            // get connection
                            System.out.println("Create JDBC connection");
                            Connection connection = getConnection();
                            System.out.println("Done creating JDBC connectionn");
                            info("Connection Successful!");
                            // create statement
                            System.out.println("Create JDBC statement");
                            Statement statement = connection.createStatement();
                            System.out.println("Done creating JDBC statementn");
                            // create a table
                            System.out.println("Create demo table");
                            statement.executeUpdate("create or replace table demo(C1 STRING)");
                            statement.close();
                            System.out.println("Done creating demo tablen");
                        } catch (Exception e) {
                            info("Failed to connect: " + e.getLocalizedMessage());
                        }
                    }
                };

        form.add(paramPanel);
        add(form);
    }

    private static Connection getConnection() throws SQLException {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException ex) {
            System.err.println("Driver not found");
        }
        // build connection properties
        Properties properties = new Properties();
        properties.put("user", "AJoachim"); // replace "" with your username
        properties.put("password", ""); // replace "" with your password
        properties.put("account", "qm09251"); // replace "" with your account name
        properties.put("db", "GENACISLIVE_DEV"); // replace "" with target database name
        properties.put("schema", "PUBLIC"); // replace "" with target schema name
        // properties.put("tracing", "on");

        // create a new connection
        String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
        // use the default connection string if it is not set in environment
        if (connectStr == null) {
            connectStr =
                    "jdbc:snowflake://qm09251.east-us-2.azure.snowflakecomputing.com"; // replace
            // accountName with your account name
        }
        return DriverManager.getConnection(connectStr, properties);
    }
}
