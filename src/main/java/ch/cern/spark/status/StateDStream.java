package ch.cern.spark.status;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

public class StateDStream<K extends StatusKey, V, S extends StatusValue, R> {

    private JavaMapWithStateDStream<K, ActionOrValue<V>, S, RemoveAndValue<K, R>> stream;

    public StateDStream(JavaMapWithStateDStream<K, ActionOrValue<V>, S, RemoveAndValue<K, R>> statusStream) {
        this.stream = statusStream;
    }
    
    public JavaDStream<R> values() {
        return stream.filter(av -> av.getValue().isPresent()).map(av -> av.getValue().get());
    }
    
    public JavaPairDStream<K, S> statuses() {
        return stream.stateSnapshots();
    }

}
