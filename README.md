# Comsonics gt-snowflake geoserver snowflake extension

run 

```
mvn clean install
```
Verify the Output:

Check the generated JAR file in the target directory to ensure it contains the expected content.
If you created a ZIP file as part of the assembly, verify its contents as well.
Deploy the Extension:

Deploy your extension to a GeoServer instance to test it. You can do this by copying the generated JAR file to the WEB-INF/lib directory of your GeoServer installation.
Restart GeoServer to load the new extension.
Test Your Extension:

Verify that your UI extension is working as expected within GeoServer.
Check for any errors in the GeoServer logs and ensure that your extension's functionality integrates smoothly with the existing GeoServer UI.
Develop Further:

Continue developing and enhancing your UI extension based on your requirements.
Update the pom.xml and assembly descriptor as needed for additional features or dependencies.
Documentation and Version Control:

Document your code and the steps required to build and deploy the extension.
Use version control (e.g., Git) to manage your codebase and track changes.
If you encounter any issues during these steps or need further assistance, feel free to ask!