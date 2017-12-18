package ch.cern.spark.status.storage.manager;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import scala.Tuple2;

public class ClassNameStatusKeyFilter implements Function<Tuple2<StatusKey, StatusValue>, Boolean> {

    private static final long serialVersionUID = -5039261513238813096L;
    private String filter_by_fqcn;

    public ClassNameStatusKeyFilter(String filter_by_fqcn) {
        this.filter_by_fqcn = filter_by_fqcn;
    }

    @Override
    public Boolean call(Tuple2<StatusKey, StatusValue> tuple) throws Exception {
        if(filter_by_fqcn == null)
            return true;
        
        StatusKey key = tuple._1;

        String name = key.getClass().getName();
        if(name.equals(filter_by_fqcn))
            return true;
        
        ClassNameAlias aliasAnno = key.getClass().getAnnotation(ClassNameAlias.class);
        
        return aliasAnno != null && aliasAnno.value().equals(filter_by_fqcn);
    }

}
