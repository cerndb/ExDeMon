package ch.cern.spark.status;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

public abstract class UpdateStatusFunction<K extends StatusKey, V, S extends StatusValue, R>
    implements Function4<Time, K, Optional<V>, State<S>, Optional<R>> {

    private static final long serialVersionUID = 8556057397769787107L;
    
    @Override
    public Optional<R> call(Time time, K key, Optional<V> value, State<S> state) throws Exception {
        if(state.isTimingOut())
            return timingOut(time, key, state);
        
        return toOptional(update(time, key, value.get(), state));
    }

    protected abstract java.util.Optional<R> update(Time time, K key, V value, State<S> state) throws Exception;

    protected Optional<R> timingOut(Time time, K key, State<S> state) {
        return Optional.empty();
    }
    
    private Optional<R> toOptional(java.util.Optional<R> result) {
        return result.isPresent() ? Optional.of(result.get()) : Optional.empty();
    }

}
