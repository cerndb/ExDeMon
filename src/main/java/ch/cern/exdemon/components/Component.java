package ch.cern.exdemon.components;

import java.io.Serializable;
import java.util.Optional;

import ch.cern.exdemon.metrics.defined.DefinedMetric;
import ch.cern.exdemon.metrics.schema.MetricSchema;
import ch.cern.exdemon.monitor.Monitor;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;
import lombok.Getter;

public abstract class Component implements Serializable {

    private static final long serialVersionUID = -2299173239147440553L;

    public enum Type {
        STATUS_STORAGE, 
        COMPONENTS_SOURCE,
        METRIC_SOURCE, 
        AGGREGATION,
        ANAYLSIS, 
        ANALYSIS_RESULTS_SINK, 
        TRIGGER,
        ACTUATOR,
        SCHEMA  { String type(){ return MetricSchema.class.getName(); }},
        METRIC  { String type(){ return DefinedMetric.class.getName(); }},
        MONITOR { String type(){ return Monitor.class.getName(); }};
        
        String type() { return null; }
    };

    private String id;
    
    @Getter
    private int propertiesHash;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    protected final ConfigurationResult buildConfig(Properties properties) throws ConfigurationException {
        propertiesHash = properties.hashCode();
        
        return config(properties);
    }

    protected ConfigurationResult config(Properties properties) {   
        return ConfigurationResult.SUCCESSFUL();
    }

    public boolean hasStatus() {
        return this instanceof HasStatus;
    }

    public Optional<StatusValue> getStatus() {
        if (hasStatus())
            return Optional.ofNullable(((HasStatus) this).save());
        else
            return Optional.empty();
    }
    
}
