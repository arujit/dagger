package com.gojek.daggers.config.system;

import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class EnvironmentConfigurationProviderTest {

    private EnvironmentConfigurationProvider environmentConfigurationProvider;

    @Before
    public void setup(){
        environmentConfigurationProvider = new EnvironmentConfigurationProvider();
    }

    @Test
    public void shouldProvideSystemConfiguration() {
        System.setProperty("key", "value");
        System.setProperty("key2", "value2");

        Configuration stringStringMap = environmentConfigurationProvider.get();

        assertEquals(stringStringMap.getString("key",""), "value");
        assertEquals(stringStringMap.getString("key2",""), "value2");
    }
}