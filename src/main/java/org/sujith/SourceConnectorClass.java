package org.sujith;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class SourceConnectorClass extends SourceConnector {

    private Map<String,String> configurationProperties = null;

    protected static final String VERSION = "1.0.0";

    @Override
    public ConfigDef config(){
        return ConfigurationSource.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SourceTaskClass.class;
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Connector Begins {}", properties);
        configurationProperties = properties;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int tasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(tasks);
        taskConfigs.add(configurationProperties);
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Connector Stops...");
    }


    @Override
    public String version() {
        return VERSION;
    }






}
