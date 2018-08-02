package ch.cern.exdemon.metrics.schema;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.utils.ExceptionsCache;
import lombok.ToString;

@ToString
@ComponentType(Type.SCHEMA)
public final class MetricSchema extends Component {

    private static final long serialVersionUID = -8885058791228553794L;

    private transient final static Logger LOG = Logger.getLogger(MetricSchema.class.getName());

    public static String SOURCES_PARAM = "sources";
    private List<String> sources;

    protected HashMap<String, String> fixedAttributes;
    
    public static String ATTRIBUTES_PARAM = "attributes";
    protected List<AttributeDescriptor> attributes;

    public static String VALUES_PARAM = "value";
    protected List<ValueDescriptor> values;
    
    public static String TIMESTAMP_PARAM = "timestamp";
    private TimestampDescriptor timestampDescriptor;    

    public static String FILTER_PARAM = "filter";
    protected MetricsFilter filter;

    private static transient ExceptionsCache exceptionsCache = new ExceptionsCache(Duration.ofMinutes(1));
    
    public MetricSchema() {
    }
    
    public MetricSchema(String id) {
        setId(id);
    }

    @Override
    public ConfigurationResult config(Properties properties) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        String sourcesValue = properties.getProperty(SOURCES_PARAM);
        if (sourcesValue == null)
            confResult.withError(SOURCES_PARAM, ConfigurationResult.MUST_BE_CONFIGURED_MSG);
        sources = Arrays.asList(sourcesValue.split("\\s"));
        
        timestampDescriptor = new TimestampDescriptor();
        confResult.merge(TIMESTAMP_PARAM, timestampDescriptor.config(properties.getSubset(TIMESTAMP_PARAM)));

        values = new LinkedList<>();
        Properties valuesProps = properties.getSubset(VALUES_PARAM);
        
        Set<String> valueIDs = valuesProps.getIDs();
        for (String valueId : valueIDs) {
            ValueDescriptor descriptor = new ValueDescriptor(valueId);
            confResult.merge(VALUES_PARAM + valueId, descriptor.config(valuesProps.getSubset(valueId)));
            
            values.add(descriptor);
        }
        if (values.isEmpty())
            confResult.withError(VALUES_PARAM , ConfigurationResult.MUST_BE_CONFIGURED_MSG);

        fixedAttributes = new HashMap<>();
        
        attributes = new LinkedList<>();
        Properties attributesProps = properties.getSubset(ATTRIBUTES_PARAM);
        attributesProps.setProperty("$schema.value", getId());
        //TODO DEPRECATED
        Set<String> oldAttributeKeys = attributesProps.keySet().stream()
                                                               .map(key -> key.toString())
                                                               .filter(key -> !key.contains("."))
                                                               .collect(Collectors.toSet());
        oldAttributeKeys.forEach(oldKey -> attributesProps.put(oldKey + ".key", attributesProps.get(oldKey)));
        //TODO DEPRECATED
        Set<String> attributeAliases = attributesProps.keySet().stream()
                                                               .map(key -> key.toString())
                                                               .filter(key -> key.contains("."))
                                                               .map(key -> key.substring(0, key.indexOf(".")))
                                                               .collect(Collectors.toSet());
        for (String attributeAlias : attributeAliases) {
            AttributeDescriptor attDescriptor = new AttributeDescriptor(attributeAlias);
            confResult.merge(ATTRIBUTES_PARAM + "." + attributeAlias, attDescriptor.config(attributesProps.getSubset(attributeAlias)));
            
            attributes.add(attDescriptor);
        }

        try {
            filter = MetricsFilter.build(properties.getSubset(FILTER_PARAM));
        } catch (ConfigurationException e) {
            confResult.withError(FILTER_PARAM, e);
        }
        
        return confResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }

    public List<Metric> call(JSON jsonObject) {        
        try {
            Map<String, String> attributesForMetric = new HashMap<>(fixedAttributes);
            for (AttributeDescriptor attributeDescriptor : attributes)
                attributesForMetric.putAll(attributeDescriptor.extract(jsonObject));
            
            if(!filter.test(attributesForMetric))
                return Collections.emptyList();

            Exception timestampException = null;
            Instant timestamp = null;
            try {
                timestamp = timestampDescriptor.extract(jsonObject);
            } catch (Exception e) {
                timestampException = e;

                timestamp = Instant.now();
            }
            
            List<Metric> metrics = new LinkedList<>();

            for (ValueDescriptor valueDescriptor : values) {
                String id = valueDescriptor.getId();

                Map<String, String> metric_ids = new HashMap<>(attributesForMetric);
                metric_ids.put("$value", id);

                if (timestampException != null) {
                    Optional<ExceptionValue> exceptionValueOpt = raiseException(id, timestampException);
                    if(exceptionValueOpt.isPresent())
                        metrics.add(new Metric(timestamp, exceptionValueOpt.get(), metric_ids));
                    
                    continue;
                }
                
                Optional<Value> value = valueDescriptor.extract(jsonObject);
                
                if(value.isPresent())
                    metrics.add(new Metric(timestamp, value.get(), metric_ids));
            }

            return metrics.stream().filter(filter).collect(Collectors.toList());
        } catch (Exception e) {
            Optional<ExceptionValue> exceptionValueOpt = raiseException(null, e);

            if(exceptionValueOpt.isPresent())
                return Collections.singletonList(new Metric(Instant.now(), exceptionValueOpt.get(), fixedAttributes));
            else
                return Collections.emptyList();
        }
    }

    private Optional<ExceptionValue> raiseException(String value, Exception exception) {
        if(!exceptionsCache.wasRecentlyRaised(getId() + value, exception)) {
            LOG.error(getId() + ": " + exception.getMessage(), exception);
            
            exceptionsCache.raised(getId() + value, exception);
            
            return Optional.of(new ExceptionValue(exception.getMessage()));
        }
        
        return Optional.empty();
    }

    public boolean containsSource(String sourceID) {
        return sources != null && sources.contains(sourceID);
    }

}
