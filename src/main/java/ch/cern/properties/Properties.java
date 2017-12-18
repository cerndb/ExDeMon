package ch.cern.properties;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import ch.cern.Cache;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.source.PropertiesSource;
import ch.cern.utils.Pair;
import ch.cern.utils.TimeUtils;
import scala.Tuple2;

public class Properties extends java.util.Properties{
	
	private static transient final long serialVersionUID = 2510326766802151233L;
	
	private transient final static Logger LOG = Logger.getLogger(Properties.class.getName());
	
	private static Cache<Properties> cachedProperties = null;
	
	private Set<String> usedKeys = new HashSet<>();

	public Properties() {
    }
	
	public Properties(Properties props) {
	    putAll(props);
	}

    public static Properties fromFile(String loadingPath) throws IOException {
    		Properties props = new Properties();
		
        FileSystem fs = FileSystem.get(new Configuration());
        InputStreamReader is = new InputStreamReader(fs.open(new Path(loadingPath)));
        props.load(is);
		is.close();
		
		return props;
	}

	public List<String> getKeysThatStartWith(String prefix) {
        return keySet().stream()
                .map(String::valueOf)
                .filter(s -> s.startsWith(prefix))
                .collect(Collectors.toList());
    }
    
	@Override
	public String getProperty(String key) {
		usedKeys.add(key);
		
		return super.getProperty(key);
	}
	
    public Properties getSubset(String topLevelKey){
        topLevelKey += ".";
        
        Properties properties = new Properties();
        
        List<String> keysWithPrefix = getKeysThatStartWith(topLevelKey);
        
        for (String keyWithPrefix : keysWithPrefix) {
            String keyWithoutPrefix = keyWithPrefix.replace(topLevelKey, "");
            
            properties.setProperty(keyWithoutPrefix, getProperty(keyWithPrefix));
        }
        
        return properties;
    }
	
	public String getProperty(String key, String defaultValue) {
		String value = getProperty(key);
		
		return value != null ? value : defaultValue;
	}

	public Set<String> getUniqueKeyFields() {
    		return keySet().stream()
		    			.map(String::valueOf)
		    			.map(s -> s.split("\\."))
		    			.filter(spl -> spl.length > 0)
		    			.map(spl -> spl[0])
		    			.distinct()
		    			.collect(Collectors.toSet());
    }

    public Float getFloat(String key) {
        String value = getProperty(key);
        
        return value == null ? null : Float.parseFloat(value);
    }
    
    public float getFloat(String key, float defaultValue) {
        Float value = getFloat(key);
        
        return value == null ? defaultValue : value;
    }

    public Long getLong(String key) {
        String value = getProperty(key);
        
        return value == null ? null : Long.parseLong(value);
    }
    
    public long getLong(String key, long defaultValue) {
    		Long value = getLong(key);
    	
    		return value == null ? defaultValue : value;
	}

    public boolean getBoolean(String key) throws ConfigurationException {
        String value = getProperty(key);
        
        if(value == null)
        		return false;
        		 
        if(value.toLowerCase().equals("true"))
        		return true;
        else if(value.toLowerCase().equals("false"))
        		return false;
        else
        		throw new ConfigurationException(key + " expects boolean value (true or false). \"" + value + "\" could not be parsed");
    }
    
    public void setPropertyIfAbsent(String key, String value){
        if(!containsKey(key))
        		setProperty(key, value);
    }
    
    public boolean isTypeDefined() {
    		return getProperty("type") != null;
    }

	public Duration getPeriod(String key, Duration periodDefault) throws ConfigurationException {
		String value = getProperty(key);
		
		if(value == null)
			return periodDefault;
		
		try{
			return TimeUtils.parsePeriod(value);
		}catch(NumberFormatException e) {
			throw new ConfigurationException("For key=" + key + ": " + e.getMessage());
		}
	}

	public Optional<Duration> getPeriod(String key) throws ConfigurationException {
		return Optional.ofNullable(getPeriod(key, null));
	}
	
	public static Cache<Properties> getCache(){
		return cachedProperties;
	}
	
	public static void resetCache() {
		Properties.cachedProperties = null;
	}
	
	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		if(Properties.cachedProperties == null)
			Properties.cachedProperties = new PropertiesCache(propertiesSourceProps);
		
		if(propertiesSourceProps != null)
			getCache().setExpiration(propertiesSourceProps.getPeriod("expire", Duration.ofMinutes(1)));
	}

	public void setDefaultPropertiesSource(String propertyFilePath) {
		Properties propertiesSourceProperties = getSubset(PropertiesSource.CONFIGURATION_PREFIX);
		
		if(!propertiesSourceProperties.containsKey("type")) {
			setProperty(PropertiesSource.CONFIGURATION_PREFIX + ".type", "file");
			setProperty(PropertiesSource.CONFIGURATION_PREFIX + ".path", propertyFilePath);
		}
	}
	
	public static class PropertiesCache extends Cache<Properties> {
		
		private Properties propertiesSourceProps;
		
		public PropertiesCache(Properties propertiesSourceProps) {
			this.propertiesSourceProps = propertiesSourceProps;
		}
		
		@Override
		protected Properties load() throws Exception {
			LOG.info("Loading properties.");
			LOG.info("Properties source parameters: " + propertiesSourceProps);
			
			if(propertiesSourceProps == null)
				return new Properties();
			
			Optional<PropertiesSource> propertiesSource = ComponentManager.buildOptional(Type.PROPERTIES_SOURCE, propertiesSourceProps);

			if(propertiesSource.isPresent()) {
				Properties properties = propertiesSource.get().load();
				
				LOG.info("Properties loaded from source.");
				
				return properties;
			}

			return new Properties();
		}
		
	}

	public void confirmAllPropertiesUsed() throws ConfigurationException {
		HashSet<Object> leftKeys = new HashSet<>(keySet());
		leftKeys.removeAll(usedKeys);
		
		if(!leftKeys.isEmpty())
			throw new ConfigurationException("Some configuration parameters ("+leftKeys+") were not used.");
	}

	public Map<String, String> toStringMap() {
		return entrySet().stream()
				.map(e -> new Pair<String, String>(e.getKey().toString(), e.getValue().toString()))
				.collect(Collectors.toMap(Pair::first, Pair::second));
	}

	public static Properties from(Tuple2<String, String>[] values) {
		Properties properties = new Properties();
		
		for (Tuple2<String, String> value : values)
			properties.setProperty(value._1, value._2);
		
		return properties;
	}
	
}
