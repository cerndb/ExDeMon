package ch.cern.spark.status;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

public abstract class UpdateStatusFunction<K extends StatusKey, V, S extends StatusValue, R>
    implements Function4<Time, K, Optional<V>, State<S>, Optional<R>> {

    private static final long serialVersionUID = 8556057397769787107L;

}
