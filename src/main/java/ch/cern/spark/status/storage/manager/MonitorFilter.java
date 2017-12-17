package ch.cern.spark.status.storage.manager;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.status.StatusValue;
import scala.Tuple2;

public class MonitorFilter<K>
    implements Function<Tuple2<K, StatusValue>, java.lang.Boolean> {

    private static final long serialVersionUID = 8279811083018699805L;
    
    private String id;

    public MonitorFilter(String id) {
        this.id = id;
    }

    @Override
    public Boolean call(Tuple2<K, StatusValue> tuple) throws Exception {
        if(id == null)
            return true;
        
        if(tuple._1.getClass().isAssignableFrom(MonitorStatusKey.class)) {
            MonitorStatusKey key = (MonitorStatusKey) tuple._1;
            
            return key.getMonitorID().equals(id);
        }
        
        if(tuple._1.getClass().isAssignableFrom(NotificatorStatusKey.class)) {
            NotificatorStatusKey key = (NotificatorStatusKey) tuple._1;
            
            return key.getMonitorID().equals(id);
        }

        return false;
    }

}
