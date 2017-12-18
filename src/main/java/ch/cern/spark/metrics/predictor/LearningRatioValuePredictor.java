package ch.cern.spark.metrics.predictor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;

import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import ch.cern.utils.TimeUtils;

public class LearningRatioValuePredictor implements Serializable {
    
    private static final long serialVersionUID = -6776886545598962207L;

    public enum Period {HOUR, DAY, WEEK};
    private Period period;
    
    private float learning_ratio;
    
    private float[] values;
    
    private float[] variance;

    public LearningRatioValuePredictor(float learning_ratio, Period period) {
        this.learning_ratio = learning_ratio;
        this.period = period;

        intialize();
    }
    
    public void setLearningRatio(float ratio){
        this.learning_ratio = ratio;
    }
    
    public void setPeriod(Period newPeriod){
        if(!newPeriod.equals(period)){
            this.period = newPeriod;
            
            intialize();
        }
    }

    private void intialize() {
        switch(period){
        case HOUR:
            values = new float[60];
            variance = new float[60];
            break;
        case DAY:
            values = new float[60 * 24];
            variance = new float[60 * 24];
            break;
        case WEEK:
            values = new float[60 * 24 * 7];
            variance = new float[60 * 24 * 7];
            break;
        }
        
        Arrays.fill(values, Float.NaN);
        Arrays.fill(variance, Float.NaN);
    }

    public void addValue(Instant timestamp, float value) {
        int position = getCorrespondingPosition(timestamp);
        
        update(position, value);
    }

    private int getCorrespondingPosition(Instant timestamp) {
    		LocalDateTime dateTime = TimeUtils.toLocalDateTime(timestamp);
    	
        int minute = dateTime.get(ChronoField.MINUTE_OF_HOUR);
        int hour = 0;
        int day_of_week = 0;
        
        switch(period){
        case WEEK:
            day_of_week = dateTime.get(ChronoField.DAY_OF_WEEK) - 1;
        case DAY:
            hour = dateTime.get(ChronoField.HOUR_OF_DAY);
        case HOUR:
        default:
        }
        
        int position = minute + hour * 60 + day_of_week * 24 * 60;
        
        return position;
    }

    private void update(int position, float newValue) {
        float previousValue = values[position];
        
        if(Float.isNaN(previousValue)){
            values[position] = newValue;
        }else{
            float squareDiff = (float) Math.pow(newValue - previousValue, 2);
            
            values[position] = previousValue * (1 - learning_ratio) + newValue * learning_ratio;
            
            if(Float.isNaN(variance[position]))
                variance[position] = squareDiff;
            else
                variance[position] = variance[position] * (1 - learning_ratio) + squareDiff * learning_ratio;
        }
    }

    public Prediction getPredictionForTime(Instant timestamp) {
        int position = getCorrespondingPosition(timestamp);
   
        if(Float.isNaN(values[position]))
            throw new RuntimeException("There are no previous values from history");
        
        float stndDev;
        if(Float.isNaN(variance[position]))
            stndDev = values[position] * 0.1f;
        else
            stndDev = (float) Math.sqrt(variance[position]);
            
        return new Prediction(values[position], stndDev);
    }
    
    @ClassNameAlias("learning-ratio")
    public static class Status_ extends StatusValue{
        private static final long serialVersionUID = 4807764662439943004L;
        
        public LearningRatioValuePredictor predictor;
        
        private void writeObject(ObjectOutputStream out) throws IOException{
            out.writeFloat(predictor.learning_ratio);
            out.writeObject(predictor.period.name());
            out.writeObject(predictor.values);
            out.writeObject(predictor.variance);
        }
        
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
            float ratio = in.readFloat();
            Period period = Period.valueOf((String) in.readObject());
            
            predictor = new LearningRatioValuePredictor(ratio, period);
            predictor.values = (float[]) in.readObject();
            predictor.variance = (float[]) in.readObject();
        }
    }

    public Period getPeriod() {
        return period;
    }

    public float getLearning_ratio() {
        return learning_ratio;
    }

    public float[] getValues() {
        return values;
    }

    public float[] getVariance() {
        return variance;
    }
    
}
