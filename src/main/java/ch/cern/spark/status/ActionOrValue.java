package ch.cern.spark.status;

import java.io.Serializable;

public class ActionOrValue<T> implements Serializable{

    private static final long serialVersionUID = 1246832289612522256L;
    
    public enum Action {REMOVE}
    private Action action;
    
    private T value;
    
    public ActionOrValue(Action action) {
        this.action = action;
    }

    public ActionOrValue(T value) {
        this.value = value;
    }
    
    public T getValue() {
        return value;
    }

    public Action getAction() {
        return action;
    }
    
}
