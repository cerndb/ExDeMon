package ch.cern.spark.metrics.notificator;

import java.util.Map;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.storage.ClassNameAlias;

@ClassNameAlias("notificator-key")
public class NotificatorStatusKey implements IDStatusKey{

    private static final long serialVersionUID = -4289498306145284346L;

    private String id;
    
    private String monitor;
    
    private Map<String, String> metric_ids;
    
    public NotificatorStatusKey(String monitorID, String notificatorID, Map<String, String> metricIDs){
        this.monitor = monitorID;
        this.id = notificatorID;
        this.metric_ids = metricIDs;
    }

    public String getMonitorID() {
        return monitor;
    }
    
    public String getNotificatorID() {
        return id;
    }
    
    public Map<String, String> getMetricIDs(){
        return metric_ids;
    }
    
    @Override
    public String getID() {
        return monitor + ":" + id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((metric_ids == null) ? 0 : metric_ids.hashCode());
        result = prime * result + ((monitor == null) ? 0 : monitor.hashCode());
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
        NotificatorStatusKey other = (NotificatorStatusKey) obj;
        if (metric_ids == null) {
            if (other.metric_ids != null)
                return false;
        } else if (!metric_ids.equals(other.metric_ids))
            return false;
        if (monitor == null) {
            if (other.monitor != null)
                return false;
        } else if (!monitor.equals(other.monitor))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }
    
    
}
