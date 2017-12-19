package ch.cern.spark.status;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

public class StatusImpl<S extends StatusValue> extends State<S> {

    private State<S> state;
    
    private Time time;

    private boolean removed;

    public StatusImpl(State<S> state, Time time) {
        this.state = state;
        this.time = time;
        
        this.removed = false;
    }

    @Override
    public boolean exists() {
        return state.exists();
    }

    @Override
    public S get() {
        return state.get();
    }

    @Override
    public void remove() {
        state.remove();
        removed = true;
    }

    @Override
    public boolean isTimingOut() {
        return state.isTimingOut();
    }

    @Override
    public void update(S newState) {
        newState.update(state, time);
    }
    
    public boolean isRemoved() {
        return removed;
    }

}
