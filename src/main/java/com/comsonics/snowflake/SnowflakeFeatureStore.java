package com.comsonics.snowflake;

import java.io.IOException;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.Query;
import org.geotools.api.data.QueryCapabilities;
import org.geotools.api.data.ResourceInfo;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.Name;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.data.store.ContentState;
import org.geotools.geometry.jts.ReferencedEnvelope;

public class SnowflakeFeatureStore extends ContentFeatureStore {
	
	public SnowflakeFeatureStore(ContentEntry entry, Query query) {
		super(entry, query);
	}
	
	 //
    // SnowflakeFeatureStore implementations
    //
    @Override
    protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(
            Query query, int flags) throws IOException {
        return new SnowflakeFeatureWriter(getState(), query);
    }
    
    /**
     * Delegate used for FeatureSource methods (We do this because Java cannot inherit from both
     * ContentFeatureStore and CSVFeatureSource at the same time
     */
    SnowflakeFeatureSource delegate =
            new SnowflakeFeatureSource(entry, query) {
                @Override
                public void setTransaction(Transaction transaction) {
                    super.setTransaction(transaction);
                    SnowflakeFeatureStore.this.setTransaction(
                            transaction); // Keep these two implementations on the same transaction
                }
            };

    @Override
    public void setTransaction(Transaction transaction) {
        super.setTransaction(transaction);
        if (delegate.getTransaction() != transaction) {
            delegate.setTransaction(transaction);
        }
    }
    
    //
    // Internal Delegate Methods
    // Implement FeatureSource methods using SnowflakeFeatureSource implementation
    //
    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return delegate.buildFeatureType();
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return delegate.getBoundsInternal(query);
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return delegate.getCountInternal(query);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        return delegate.getReaderInternal(query);
    }
    
    //
    // Public Delegate Methods
    // Implement FeatureSource methods using CSVFeatureSource implementation
    //
    @Override
    public SnowflakeDataStore getDataStore() {
        return delegate.getDataStore();
    }

    @Override
    public ContentEntry getEntry() {
        return delegate.getEntry();
    }

    public Transaction getTransaction() {
        return delegate.getTransaction();
    }

    public ContentState getState() {
        return delegate.getState();
    }

    public ResourceInfo getInfo() {
        return delegate.getInfo();
    }

    public Name getName() {
        return delegate.getName();
    }

    public QueryCapabilities getQueryCapabilities() {
        return delegate.getQueryCapabilities();
    }

}
