package com.comsonics.snowflake;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.cs.AxisDirection;
import org.geotools.data.DataUtilities;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;


public class SnowflakeFeatureWriter implements FeatureWriter<SimpleFeatureType, SimpleFeature> {

	/** State of current transaction */
    private ContentState state;

    /** Delegate handing reading of original file */
    private SnowflakeFeatureReader delegate;

    /** Temporary file used to stage output */
    private File temp;

    /** CsvWriter used for temp file output */
    //private SnowflakeWriter snowflakeWriter;

    /** Current feature available for modification, may be null if feature removed */
    private SimpleFeature currentFeature;

    /** Flag indicating we have reached the end of the file */
    private boolean appending = false;

    /** flag to keep track of lat/lon order */
    private boolean latlon =
            DefaultGeographicCRS.WGS84
                    .getCoordinateSystem()
                    .getAxis(0)
                    .getDirection()
                    .equals(AxisDirection.NORTH);

    int latIndex = 0;
    int lngIndex = 0;
    /** Row count used to generate FeatureId when appending */
    int nextRow = 0;
    
    public SnowflakeFeatureWriter(ContentState state, Query query) throws IOException {
        this.state = state;
        String typeName = query.getTypeName();
        //File file = ((SnowflakeDataStore) state.getEntry().getDataStore()).file;
        //File directory = file.getParentFile();
        //this.temp = File.createTempFile(typeName + System.currentTimeMillis(), "csv", directory);
        //this.snowflakeWriter = new SnowflakeWriter(new FileWriter(this.temp), ',');
        this.delegate = new SnowflakeFeatureReader(state, query);
        //this.snowflakeWriter.writeRecord(delegate.reader.getHeaders());
        String latField = "lat";
        String lngField = "lon";
        /*
        String[] headers = delegate.reader.getHeaders();
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equalsIgnoreCase(latField)) {
                latIndex = i;
            }
            if (headers[i].equalsIgnoreCase(lngField)) {
                lngIndex = i;
            }
        }
        */
    }
    
    @Override
    public SimpleFeatureType getFeatureType() {
        return state.getFeatureType();
    }
    
    @Override
    public boolean hasNext() throws IOException {
        /*if (snowflakeWriter == null) {
            return false;
        }*/
        if (this.appending) {
            return false; // reader has no more contents
        }
        return delegate.hasNext();
    }
    
    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {
        /*if (snowflakeWriter == null) {
            throw new IOException("FeatureWriter has been closed");
        }
        */
        if (this.currentFeature != null) {
            this.write(); // the previous one was not written, so do it now.
        }
        try {
            if (!appending) {
                if (/*delegate.reader != null &&*/ delegate.hasNext()) {
                    this.currentFeature = delegate.next();
                    return this.currentFeature;
                } else {
                    this.appending = true;
                }
            }
            SimpleFeatureType featureType = state.getFeatureType();
            String fid = featureType.getTypeName() + "." + nextRow;
            Object[] values = DataUtilities.defaultValues(featureType);

            this.currentFeature = SimpleFeatureBuilder.build(featureType, values, fid);
            return this.currentFeature;
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Unable to create feature:" + invalid.getMessage(), invalid);
        }
    }
    
    /**
     * Mark our {@link #currentFeature} feature as null, it will be skipped when written effectively
     * removing it.
     */
    public void remove() throws IOException {
        this.currentFeature = null; // just mark it done which means it will not get written out.
    }
    
    public void write() throws IOException {
    	return;
    	/*
        if (this.currentFeature == null) {
            return; // current feature has been deleted
        }
        for (Property property : currentFeature.getProperties()) {
            Object value = property.getValue();
            if (value == null) {
                this.snowflakeWriter.write("");
            } else if (value instanceof Point) {
                Point point = (Point) value;
                if (latlon && latIndex <= lngIndex) {
                    this.snowflakeWriter.write(Double.toString(point.getX()));
                    this.snowflakeWriter.write(Double.toString(point.getY()));
                } else {
                    this.snowflakeWriter.write(Double.toString(point.getY()));
                    this.snowflakeWriter.write(Double.toString(point.getX()));
                }
            } else {
                String txt = value.toString();
                this.snowflakeWriter.write(txt);
            }
        }
        this.snowflakeWriter.endRecord();
        nextRow++;
        this.currentFeature = null; // indicate that it has been written
        */
    }
    
    @Override
    public void close() throws IOException {
        /*if (snowflakeWriter == null) {
            throw new IOException("Writer alread closed");
        }*/
        if (this.currentFeature != null) {
            this.write(); // the previous one was not written, so do it now.
        }
        // Step 1: Write out remaining contents (if applicable)
        while (hasNext()) {
            next();
            write();
        }
        //snowflakeWriter.close();
        //snowflakeWriter = null;
        if (delegate != null) {
            this.delegate.close();
            this.delegate = null;
        }
        // Step 2: Replace file contents
        //File file = ((SnowflakeDataStore) state.getEntry().getDataStore()).file;

        //Files.copy(temp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
}
