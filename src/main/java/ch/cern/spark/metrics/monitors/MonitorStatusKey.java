package ch.cern.spark.metrics.monitors;

import java.util.Map;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.storage.JSONSerializationClassNameAlias;

@JSONSerializationClassNameAlias("monitor-key")
public class MonitorStatusKey implements IDStatusKey{
    
    private static final long serialVersionUID = -7288054142035095969L;

    private String id;
    
    private Map<String, String> metric_ids;
    
    public MonitorStatusKey(String monitorID, Map<String, String> metricIDs){
        this.id = monitorID;
        this.metric_ids = metricIDs;
    }

    public String getID() {
        return id;
    }
    
    public Map<String, String> getMetricIDs(){
        return metric_ids;
    }

    @Override
    public String toString() {
        return "MonitorIDMetricIDs [monitorID=" + id + ", metricIDs=" + metric_ids + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((metric_ids == null) ? 0 : metric_ids.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MonitorStatusKey other = (MonitorStatusKey) obj;
        if (metric_ids == null) {
            if (other.metric_ids != null)
                return false;
        } else if (!metric_ids.equals(other.metric_ids))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

}
