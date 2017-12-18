package ch.cern.spark.status.storage.manager;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import scala.Tuple2;

public class IDStatusKeyFilter implements Function<Tuple2<StatusKey, StatusValue>, Boolean> {

    private static final long serialVersionUID = -8299090750744907416L;
    
    private String filter_by_id;

    public IDStatusKeyFilter(String filter_by_id) {
        this.filter_by_id = filter_by_id;
    }

    @Override
    public Boolean call(Tuple2<StatusKey, StatusValue> tuple) throws Exception {
        if(filter_by_id == null)
            return true;
        
        StatusKey key = tuple._1;
        
        if(!(key instanceof IDStatusKey))
            return false;
        
        IDStatusKey idKey = (IDStatusKey) key;
        
        return idKey.getID().equals(filter_by_id);
    }

}
