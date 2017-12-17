package ch.cern.spark.status.storage.manager;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.status.StatusValue;
import scala.Tuple2;

public class NotificatorFilter<K>
        implements Function<scala.Tuple2<K, ch.cern.spark.status.StatusValue>, java.lang.Boolean> {

    private static final long serialVersionUID = 7590416563523644987L;
    private String id;

    public NotificatorFilter(String id) {
        this.id = id;
    }

    @Override
    public Boolean call(Tuple2<K, StatusValue> tuple) throws Exception {
        if(id == null)
            return true;
        
        if(!tuple._1.getClass().isAssignableFrom(NotificatorStatusKey.class))
            return false;
        
        NotificatorStatusKey key = (NotificatorStatusKey) tuple._1;
        
        return key.getNotificatorID().equals(id);
    }

}
