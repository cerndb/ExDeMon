package ch.cern.spark.metrics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import ch.cern.utils.TimeUtils;

public class ValueHistory implements Serializable {

    private static final long serialVersionUID = 9141577304066319408L;

    private List<DatedValue> values;
    
    private Duration period = Duration.ofMinutes(30);

    public ValueHistory(Duration period){
        this.values = new LinkedList<>();
        
        this.period = period;
    }
    
    public void setPeriod(Duration period) {
        this.period = period;
    }

    public void add(Instant time, float value) {
        values.add(new DatedValue(time, new FloatValue(value)));
    }
    
    public void add(Instant time, Value value) {
        values.add(new DatedValue(time, value));
    }
    
    public void purge(Instant time) {
    		Instant oldest_time = time.minus(period);
        
    		values.removeIf(value -> value.getInstant().isBefore(oldest_time));
    }

    public int size() {
        return values.size();
    }

    public List<DatedValue> getDatedValues() {
        return values;
    }

    @Override
    public String toString() {
        return "ValueHistory [metrics=" + values + ", period=" + period + "]";
    }

    public List<Value> getHourlyValues(Instant time) {
    		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
    	
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), false, false))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

	public List<Value> getDaylyValues(Instant time) {
		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
		
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), false, true))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }

    public List<Value> getWeeklyValues(Instant time) {
    		LocalDateTime dateTime = TimeUtils.toLocalDateTime(time);
    		
        return values.stream()
        		.filter(value -> isSameMinute(dateTime, TimeUtils.toLocalDateTime(value.getInstant()), true, true))
        		.map(value -> value.getValue())
        		.collect(Collectors.toList());
    }
    
    private boolean isSameMinute(LocalDateTime time1, LocalDateTime time2, boolean week, boolean day) {
		return time1.get(ChronoField.MINUTE_OF_HOUR) == time2.get(ChronoField.MINUTE_OF_HOUR)
				&& (!day || time1.get(ChronoField.HOUR_OF_DAY) == time2.get(ChronoField.HOUR_OF_DAY))
				&& (!week || time1.get(ChronoField.DAY_OF_WEEK) == time2.get(ChronoField.DAY_OF_WEEK));
	}

    public DescriptiveStatistics getStatistics() {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        
        values.stream()
        		.map(val -> val.getValue().getAsFloat())
        		.filter(Optional::isPresent).map(Optional::get)
        		.forEach(stats::addValue);
        
        return stats;
    }
    
    @ClassNameAlias("value-history")
    public static class Status extends StatusValue{
        private static final long serialVersionUID = 8818532585911816073L;
        
        public ValueHistory history;
        
        private void writeObject(ObjectOutputStream out) throws IOException{
            List<DatedValue> datedValues = history.getDatedValues();
            
            long period = history.getPeriod().getSeconds();
            
            int[] times = new int[datedValues.size()];
            Value[] values = new Value[datedValues.size()];
            
            int i = 0;
            for (DatedValue value : datedValues) {
                times[i] = (int) value.getInstant().getEpochSecond();
                values[i] = value.getValue();
                
                i++;
            }
            
            out.writeLong(period);
            out.writeObject(times);
            out.writeObject(values);
        }
        
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
            long period = in.readLong();
            history = new ValueHistory(Duration.ofSeconds(period));
            
            int[] times = (int[]) in.readObject();
            Value[] values = (Value[]) in.readObject();
            
            List<DatedValue> datedValues = IntStream.range(0, times.length)
								            		.mapToObj(i -> new DatedValue(Instant.ofEpochSecond(times[i]), values[i]))
								            		.collect(Collectors.toList());
            
            history.setDatedValues(datedValues);
        }
        
    }

    public void reset() {
        this.values = new LinkedList<>();
    }

    public void setDatedValues(List<DatedValue> newValues) {
        this.values = newValues;
    }

    public Duration getPeriod() {
        return period;
    }
    
}
