package ch.cern.spark.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;

import ch.cern.components.RegisterComponentType;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

@RegisterComponentType("count_false")
public class CountFalsesAgregation extends Aggregation {

    private static final long serialVersionUID = 8274088208286521725L;

    @Override
    public Class<? extends Value> inputType() {
        return BooleanValue.class;
    }

    @Override
    public Value aggregate(Collection<DatedValue> values, Instant time) {
        return new FloatValue(values.stream()
                                        .filter(v -> !v.getValue().getAsBoolean().get() || v.getValue().getAsAggregated().isPresent())
                                        .mapToDouble(v -> v.getValue().getAsAggregated().isPresent() ? 
                                                                v.getValue().getAsAggregated().get().getAsFloat().get() : 
                                                                1)
                                        .sum());
    }

    @Override
    public Class<? extends Value> returnType() {
        return FloatValue.class;
    }

}
