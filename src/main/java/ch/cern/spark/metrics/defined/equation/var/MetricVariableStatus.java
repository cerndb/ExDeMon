package ch.cern.spark.metrics.defined.equation.var;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;

@ClassNameAlias("metric-variable")
public class MetricVariableStatus extends StatusValue {

    private static final long serialVersionUID = -7439047274576894171L;

    public static final int MAX_AGGREGATION_SIZE = 100000;

    private Map<Integer, DatedValue> aggregationValues;
    private List<DatedValue> alongTimeValues;

    public MetricVariableStatus() {
        aggregationValues = new LinkedHashMap<>();
        alongTimeValues = new LinkedList<>();
    }

    public Value getValue(Optional<Instant> oldestMetricAt, Optional<Instant> newestMetricAt) {
        List<DatedValue> values = getAggregatedDatedValues(newestMetricAt);
        
        if(values.isEmpty())
            return new ExceptionValue("no value during validity period");
        
        return values.stream().reduce((a, b) -> a.getInstant().compareTo(b.getInstant()) > 0 ? a : b).get().getValue();
    }
    
    public void add(Value value, Instant instant) {
        add(0, value, instant);
    }

    public void add(int idHash, float value, Instant instant) {
        add(idHash, new FloatValue(value), instant);
    }

    public void add(int idHash, Value value, Instant instant) {
        value = Value.clone(value);

        if (emptyAttrbutes(idHash)) {
            // Removing the oldest entry if max size
            if (alongTimeValues.size() >= MAX_AGGREGATION_SIZE)
                alongTimeValues.remove(alongTimeValues.iterator().next());

            alongTimeValues.add(new DatedValue(instant, value));
        } else {
            // Removing the oldest entry if max size
            if (aggregationValues.size() >= MAX_AGGREGATION_SIZE)
                aggregationValues.remove(aggregationValues.keySet().iterator().next());

            aggregationValues.put(idHash, new DatedValue(instant, value));
        }
    }

    private boolean emptyAttrbutes(int idHash) {
        return idHash == 0;
    }

    public List<Value> getAggregatedValues(Optional<Instant> newestMetricAt) {
        List<Value> allValues = new LinkedList<>();

        allValues.addAll(aggregationValues.values().stream()
                                                .filter(datedValue -> !newestMetricAt.isPresent() || datedValue.getInstant().compareTo(newestMetricAt.get()) < 0)
                                                .map(DatedValue::getValue)
                                                .collect(Collectors.toList()));

        allValues.addAll(alongTimeValues.stream()
                                                .filter(datedValue -> !newestMetricAt.isPresent() || datedValue.getInstant().compareTo(newestMetricAt.get()) < 0)
                                                .map(DatedValue::getValue)
                                                .collect(Collectors.toList()));

        return allValues;
    }

    public List<DatedValue> getAggregatedDatedValues(Optional<Instant> newestMetricAt) {
        List<DatedValue> allValues = new LinkedList<>();

        allValues.addAll(aggregationValues.values().stream()
                                                .filter(datedValue -> !newestMetricAt.isPresent() || datedValue.getInstant().compareTo(newestMetricAt.get()) < 0)
                                                .collect(Collectors.toList()));

        allValues.addAll(alongTimeValues.stream()
                                                .filter(datedValue -> !newestMetricAt.isPresent() || datedValue.getInstant().compareTo(newestMetricAt.get()) < 0)
                                                .collect(Collectors.toList()));

        return allValues;
    }

    public void purge(String variableID, Optional<Instant> oldestUpdateOpt) {
        if (!oldestUpdateOpt.isPresent())
            return;

        Instant oldestUpdate = oldestUpdateOpt.get();

        if (aggregationValues != null)
            aggregationValues.values().removeIf(value -> value.getInstant().isBefore(oldestUpdate));

        if (alongTimeValues != null)
            alongTimeValues.removeIf(value -> value.getInstant().isBefore(oldestUpdate));
    }

    @Override
    public String toString() {
        return "MetricVariableStore [alongTimeValues=" + alongTimeValues + ", aggregationValues=" + aggregationValues + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aggregationValues == null) ? 0 : aggregationValues.hashCode());
        result = prime * result + ((alongTimeValues == null) ? 0 : alongTimeValues.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricVariableStatus other = (MetricVariableStatus) obj;
        if (aggregationValues == null) {
            if (other.aggregationValues != null)
                return false;
        } else if (!aggregationValues.equals(other.aggregationValues))
            return false;
        if (alongTimeValues == null) {
            if (other.alongTimeValues != null)
                return false;
        } else if (!alongTimeValues.equals(other.alongTimeValues))
            return false;
        return true;
    }

}
