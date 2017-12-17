package ch.cern.spark.status.storage.manager;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.status.StatusValue;
import scala.Tuple2;

public class DefinedMetricFilter<K>
        implements Function<scala.Tuple2<K, ch.cern.spark.status.StatusValue>, java.lang.Boolean> {

    private static final long serialVersionUID = -8451806285001748524L;
    
    private String id;

    public DefinedMetricFilter(String id) {
        this.id = id;
    }

    @Override
    public Boolean call(Tuple2<K, StatusValue> tuple) throws Exception {
        if(id == null)
            return true;
        
        if(!tuple._1.getClass().isAssignableFrom(DefinedMetricStatuskey.class))
            return false;
        
        DefinedMetricStatuskey key = (DefinedMetricStatuskey) tuple._1;
        
        return key.getDefinedMetricName().equals(id);
    }

}
