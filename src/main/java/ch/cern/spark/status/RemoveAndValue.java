package ch.cern.spark.status;

import java.io.Serializable;

import org.apache.spark.api.java.Optional;

public class RemoveAndValue<K, V> implements Serializable{

    private static final long serialVersionUID = 625633246879682831L;
    
    private K key;
    
    private V value;
    
    public RemoveAndValue(K key, Optional<V> value) {
        this.key = key;
        this.value = value.isPresent() ? value.get() : null;
    }

    public boolean isRemoveAction() {
        return key != null;
    }

    public K getKey() {
        return key;
    }

    public Optional<V> getValue() {
        return Optional.fromNullable(value);
    }

}
