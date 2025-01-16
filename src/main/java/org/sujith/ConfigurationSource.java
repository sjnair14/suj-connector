package org.sujith;

import org.apache.kafka.common.config.ConfigDef;

public class ConfigurationSource {

    public static final String POLL_INTERVAL= "poll.interval";
    public static final String DEFAULT_PARTITION= "default.partition";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(POLL_INTERVAL, ConfigDef.Type.INT,10000,ConfigDef.Range.atLeast(1000), ConfigDef.Importance.HIGH,"Polling interval between two consecutive polls")
            .define(DEFAULT_PARTITION, ConfigDef.Type.INT,1, ConfigDef.Range.between(1,10), ConfigDef.Importance.HIGH,"Default Partition");
}
