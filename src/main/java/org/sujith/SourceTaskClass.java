package org.sujith;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;


import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.stream.Collectors;

@Slf4j
public class SourceTaskClass extends SourceTask {
    private Timer timer;

    private DataGetter dataGetter;
    private RecordSchemaCreator recordSchemaCreator;

    @Override
    public void start(Map<String, String> properties){

        log.info("Task begins...");

        AbstractConfig abstractConfig = new AbstractConfig(ConfigurationSource.CONFIG_DEF, properties);
        int pollInterval = abstractConfig.getInt(ConfigurationSource.POLL_INTERVAL);
        recordSchemaCreator= new RecordSchemaCreator(abstractConfig);
        Integer offset = recordSchemaCreator.getSavedOffset(getOffsetStorageReader());

        try {
            dataGetter= new DataGetter(abstractConfig,offset);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        timer= new Timer();
        timer.scheduleAtFixedRate(dataGetter,0,pollInterval);
    }

    @Override
    public void stop(){
        log.info("Task stops...");
        if(timer != null){
            timer.cancel();
        }
        timer= null;
        dataGetter = null;
        recordSchemaCreator= null;
    }
    @Override
    public String version() {
        return SourceConnectorClass.VERSION;
    }
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return dataGetter.getData()
                .stream()
                .map(r -> recordSchemaCreator.createSourceRecord(r))
                .collect(Collectors.toList());
    }

    private OffsetStorageReader getOffsetStorageReader () {
        if (context == null) {
            log.debug("No context - assuming that this is the first time the Connector has run");
            return null;
        }
        else if (context.offsetStorageReader() == null) {
            log.debug("No offset reader - assuming that this is the first time the Connector has run");
            return null;
        }
        else {
            return context.offsetStorageReader();
        }
    }
}
