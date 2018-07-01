package ch.cern.components.source;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.util.Optional;

import ch.cern.components.ComponentsCatalog;
import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.source.types.TestComponentsSource;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class ComponentsSourceTests {
    
    @Before
    public void setUp() throws Exception {
        Properties componentsSourceProperties = new Properties();
        componentsSourceProperties.setProperty("type", "test");
        ComponentsCatalog.init(componentsSourceProperties);
    }
    
    @Test
    public void register() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "id", properties);
        
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
    }
    
    @Test
    public void remove() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "id", properties);
        
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
        
        source.remove(Type.MONITOR, "id");
        
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
    }
    
    @Test
    public void okConfiguration() throws ConfigurationException {
        TestComponentsSource source = spy(new TestComponentsSource());
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        Optional<Component> componentOpt = source.register(Type.MONITOR, "id", properties);
        
        verify(source).registerConfigurationOK(eq(Type.MONITOR), eq("id"), same(componentOpt.get()));
    }
    
    @Test
    public void errorConfiguration() throws ConfigurationException {
        TestComponentsSource source = spy(new TestComponentsSource());
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("analysis.type", "no-exist");
        Optional<Component> componentOpt = source.register(Type.MONITOR, "id", properties);
        
        assertFalse(componentOpt.isPresent());
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
        verify(source).registerConfigurationError(eq(Type.MONITOR), eq("id"), any());
    }
    
    @Test
    public void idFilter() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        sourceProps.setProperty("id.filters.qa", "qa_.*");
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "id", properties);
        
        properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "qa_id", properties);
        
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "qa_id").isPresent());
    }

}
