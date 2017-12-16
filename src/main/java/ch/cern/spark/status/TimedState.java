package ch.cern.spark.status;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

public class TimedState<S extends StatusValue> extends State<S> {

    private State<S> state;
    
    private Time time;

    public TimedState(State<S> state, Time time) {
        this.state = state;
        this.time = time;
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
    }

    @Override
    public boolean isTimingOut() {
        return state.isTimingOut();
    }

    @Override
    public void update(S newState) {
        newState.update(state, time);
    }

}
