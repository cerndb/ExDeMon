package ch.cern.spark.metrics.schema;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.utils.Pair;

public class MetricSchema implements Serializable {

    private static final long serialVersionUID = -8885058791228553794L;

    private transient final static Logger LOG = Logger.getLogger(MetricSchema.class.getName());

    private String id;

    public static String SOURCES_PARAM = "sources";
    private List<String> sources;

    public static String ATTRIBUTES_PARAM = "attributes";
    private List<Pair<String, String>> attributes;

    public static String VALUE_ATTRIBUTES_PARAM = "value.keys";
    private List<Pair<String, String>> value_attributes;

    public static String TIMESTAMP_FORMAT_PARAM = "timestamp.format";
    public static String TIMESTAMP_FORMAT_DEFAULT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private String timestamp_format_pattern;
    private transient DateTimeFormatter timestamp_format;

    public static String TIMESTAMP_ATTRIBUTE_PARAM = "timestamp.key";
    private String timestamp_attribute;

    public static String FILTER_PARAM = "filter";
    private MetricsFilter filter;

    private Exception configurationException;

    public MetricSchema(String id) {
        this.id = id;
    }

    public MetricSchema config(Properties properties) {
        try {
            tryConfig(properties);
        } catch (Exception e) {
            configurationException = e;
        }

        return this;
    }

    public MetricSchema tryConfig(Properties properties) throws ConfigurationException {
        String sourcesValue = properties.getProperty(SOURCES_PARAM);
        if (sourcesValue == null)
            throw new ConfigurationException("sources must be spcified");
        sources = Arrays.asList(sourcesValue.split("\\s"));

        timestamp_attribute = properties.getProperty(TIMESTAMP_ATTRIBUTE_PARAM);
        if (timestamp_attribute == null)
            throw new ConfigurationException(TIMESTAMP_ATTRIBUTE_PARAM + " must be configured.");

        timestamp_format_pattern = properties.getProperty(TIMESTAMP_FORMAT_PARAM, TIMESTAMP_FORMAT_DEFAULT);
        if (timestamp_format_pattern != null && !timestamp_format_pattern.equals("epoch-ms")
                && !timestamp_format_pattern.equals("epoch-s"))
            try {
                new DateTimeFormatterBuilder().appendPattern(timestamp_format_pattern).toFormatter()
                        .withZone(ZoneOffset.systemDefault());
            } catch (Exception e) {
                throw new ConfigurationException(TIMESTAMP_FORMAT_PARAM
                        + " must be epoch-ms, epoch-s or a pattern compatible with DateTimeFormatterBuilder.");
            }

        value_attributes = new LinkedList<>();
        String value_attributes_value = properties.getProperty(VALUE_ATTRIBUTES_PARAM);
        if (value_attributes_value != null) {
            String[] attributesValues = value_attributes_value.split("\\s");

            for (String attribute : attributesValues)
                value_attributes.add(new Pair<String, String>(attribute, attribute));
        }
        Properties valueAttributesWithAlias = properties.getSubset(VALUE_ATTRIBUTES_PARAM);
        for (Map.Entry<Object, Object> pair : valueAttributesWithAlias.entrySet()) {
            String alias = (String) pair.getKey();
            String key = (String) pair.getValue();

            value_attributes.add(new Pair<String, String>(alias, key));
        }
        if (value_attributes.isEmpty())
            throw new ConfigurationException(VALUE_ATTRIBUTES_PARAM + " must be configured.");

        attributes = new LinkedList<>();
        String attributesValue = properties.getProperty(ATTRIBUTES_PARAM);
        if (attributesValue != null) {
            String[] attributesValues = attributesValue.split("\\s");

            for (String attribute : attributesValues)
                attributes.add(new Pair<String, String>(attribute, attribute));
        }
        Properties attributesWithAlias = properties.getSubset(ATTRIBUTES_PARAM);
        for (Map.Entry<Object, Object> pair : attributesWithAlias.entrySet()) {
            String alias = (String) pair.getKey();
            String key = (String) pair.getValue();

            attributes.add(new Pair<String, String>(alias, key));
        }

        filter = MetricsFilter.build(properties.getSubset(FILTER_PARAM));

        return this;
    }

    public List<Metric> call(JSONObject jsonObject) {
        List<Metric> metrics = new LinkedList<>();

        Map<String, String> ids = new HashMap<>();
        ids.put("$schema", id);

        try {
            if (configurationException != null)
                throw configurationException;

            Map<String, String> idsForMetric = new HashMap<>(ids);
            for (Pair<String, String> attribute : attributes) {
                String alias = attribute.first;
                String key = attribute.second;

                JsonElement value = jsonObject.getElement(key);

                if (value != null && value.isJsonPrimitive())
                    idsForMetric.put(alias, value.getAsString());
            }

            Exception timestampException = null;
            String timestamp_string = jsonObject.getProperty(timestamp_attribute);
            Instant timestamp;
            try {
                timestamp = toDate(timestamp_string);
            } catch (Exception e) {
                timestampException = new Exception("DateTimeParseException: " + e.getMessage() + " for key "
                        + timestamp_attribute + " with value (" + timestamp_string + ")");

                timestamp = Instant.now();
            }

            for (Pair<String, String> value_attribute : value_attributes) {
                String alias = value_attribute.first;
                String key = value_attribute.second;

                Map<String, String> metric_ids = new HashMap<>(idsForMetric);
                metric_ids.put("$value_attribute", alias);

                if (timestampException != null) {
                    metrics.add(new Metric(timestamp, new ExceptionValue(timestampException.getMessage()), metric_ids));
                    continue;
                }

                JsonElement element = jsonObject.getElement(key);
                Value value = null;
                if (element == null || element.isJsonNull()) {
                    value = new ExceptionValue("No metric was generated for value key \"" + key + "\" (alias: " + alias
                            + "): document does not contian such key or is null.");
                } else if (element.isJsonPrimitive()) {
                    JsonPrimitive primitive = element.getAsJsonPrimitive();

                    if (primitive.isNumber())
                        value = new FloatValue(primitive.getAsFloat());
                    else if (primitive.isBoolean())
                        value = new BooleanValue(primitive.getAsBoolean());
                    else
                        value = new StringValue(primitive.getAsString());
                } else {
                    value = new ExceptionValue("No metric was generated for value key \"" + key + "\" (alias: " + alias
                            + "): attribute is not a JSON primitive.");
                }

                metrics.add(new Metric(timestamp, value, metric_ids));
            }

            return metrics.stream().filter(filter).collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

            ExceptionValue exception = new ExceptionValue(e.getMessage());

            metrics.add(new Metric(Instant.now(), exception, ids));
            return metrics;
        }
    }

    private Instant toDate(String date_string) throws DateTimeParseException {
        if (date_string == null || date_string.length() == 0)
            throw new DateTimeParseException("No data to parse", "", 0);

        try {
            if (timestamp_format_pattern.equals("epoch-ms"))
                return Instant.ofEpochMilli(Long.valueOf(date_string));

            if (timestamp_format_pattern.equals("epoch-s"))
                return Instant.ofEpochSecond(Long.valueOf(date_string));
        } catch (Exception e) {
            throw new DateTimeParseException(e.getClass().getName() + ": " + e.getMessage(), date_string, 0);
        }

        if (timestamp_format == null)
            timestamp_format = new DateTimeFormatterBuilder()
                                            .appendPattern(timestamp_format_pattern)
                                            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                                            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                            .toFormatter();
        
        TemporalAccessor temporalAccesor = timestamp_format.parse(date_string);
        
        if (temporalAccesor.isSupported(ChronoField.INSTANT_SECONDS))
            return Instant.from(temporalAccesor);
        else
            return LocalTime.from(temporalAccesor).atOffset(OffsetDateTime.now().getOffset()).atDate(LocalDate.from(temporalAccesor)).toInstant();
    }

    public String getID() {
        return id;
    }

    public boolean containsSource(String sourceID) {
        return sources.contains(sourceID);
    }

    @Override
    public String toString() {
        return "MetricSchema [id=" + id + ", sources=" + sources + ", attributes=" + attributes + ", value_attributes="
                + value_attributes + ", timestamp_format=" + timestamp_format + ", timestamp_attribute="
                + timestamp_attribute + ", filter=" + filter + ", configurationException=" + configurationException
                + "]";
    }

}
