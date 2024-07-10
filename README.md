# Comsonics gt-snowflake geoserver snowflake extension

# Build the Project 
```
mvn clean install
```

# Verify the Output:
Check the generated JAR file in the target directory to ensure it contains the gt-jdbc-snowflake-*.jar file.
If you created a ZIP file as part of the assembly, verify its contents as well.


# Deploy the Extension:
1. Copy the generated gt-jdbc-snowflake-*.jar file to the WEB-INF/lib directory of your GeoServer installation.
2. Follow the [Snowflake Documentation](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc), or download the latest snowflake-jdbc-*.jar file directly from the [Maven Repository](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc) and copy it into the WEB-INF/lib directory of your GeoServer installation.
3. **Restart GeoServer to load the new extension.**


# Test the Extension:
1. Log into GeoServer
2. Stores -> Add New Store -> Snowflake - Snowflake Database
3. Enter Datastore information
4. Create a layer and verify it in Layer Preview
5. Check for any errors in the GeoServer logs and ensure that the extension's functionality integrates smoothly with the existing GeoServer UI.

# Database Information:
	- Data Source Name (required) - Name that GeoServer will use to display the datastore
	- Description (optional) - Short description of the datastore's purpose
	- dbtype (required) - Should be auto-filled with a value of snowflake, leave this alone
	- database (required) - Name of the Snowflake database to connect to
	- schema (optional) - Name of the schema in the database specified in step 3d
	- user (required) - Username of the Snowflake account to connect to
	- passwd (required) - Password of the Snowflake account to connect to
	- Session startup SQL (optional, but required for previewing and downloading results) - Code to run at the start of a session with this datastore
	-- In order to preview and download results, include the following as the first line of the code
	--- ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'
	- Session close-up SQL (optional) - Code to run when closing a session with this datastore
	-- If you added the code from 3h, include the following as the first line of code to revert the session variable back to its default
	---ALTER SESSION UNSET JDBC_QUERY_RESULT_FORMAT
	- account (required) - Account identifier of the Snowflake account to connect to (See Finding Your Snowflake Account Identifier for help)
	- cloud provider (required) - Cloud provider of the Snowflake account to connect to (azure, gcp, aws) (See Method 1 of note to find cloud provider)
	- cloud region (required) - Cloud region of the Snowflake account to connect to (i.e. west-us-2) (See Method 1 of note to find cloud region

# Finding Your Snowflake Account Identifier
Method 1:


	- Go to app.snowflake.com (If you're redirected to your account's SnowSight dashboard, log out)
	- Choose the Snowflake account to get the identifier for. Do not log in when prompted for username and password
	- Once you're prompted for a username and password, look at the url in the search bar
	- It should be in the format <account-identifier>.<cloud-region>.<cloud-provider>.snowflakecomputing.com
	- Copy the <account-identifier> portion of the url into the account parameter in GeoServer
	
Method 2:


	- Sign into SnowSight at app.snowflake.com
	- Click your user account in the bottom-left to bring up the user menu
	- Hover over the Account section to bring up a menu with all account your user has access to. The account you're currently signed into should have a check mark next to it
	- Hover over the Account you're signed into to bring up the Account Info menu
	- The Locator is your account identifier, copy this into the account parameter in GeoServer


# Develop Further:
Continue developing and enhancing the UI extension based on your requirements.
Update the pom.xml and assembly descriptor as needed for additional features or dependencies.


# Documentation and Version Control:
Document any code changes and adhere to GeoServer's best practices for developing extensions 
Use version control (e.g., Git) to manage your codebase and track changes.
If you encounter any issues during these steps or need further assistance, feel free to ask!