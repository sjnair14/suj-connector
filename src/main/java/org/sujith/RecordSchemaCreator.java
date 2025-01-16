package org.sujith;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collections;
import java.util.Map;

@Slf4j
public class RecordSchemaCreator {

    private int defaultPartition;
    private static final Schema DATA_SCHEMA = SchemaBuilder.struct()
            .field("userId", Schema.INT32_SCHEMA)
            .field("id",  Schema.INT32_SCHEMA)
            .field("title", Schema.STRING_SCHEMA)
            .field("body", Schema.STRING_SCHEMA)
            .build();

    public RecordSchemaCreator(AbstractConfig config) {
        defaultPartition = config.getInt(ConfigurationSource.DEFAULT_PARTITION);
    }

    private Map<String, Object> sourcePartition() {
        return Collections.singletonMap("userId", defaultPartition);
    }
    private Map<String, Object> sourceOffset(JsonData jsonData) {
        return Collections.singletonMap("id", jsonData.getId());
    }

    private String topicValue(){
        return "outputTopic";
    }

    private Struct createStruct(JsonData jsonData){
        Struct outJsonData = new Struct(DATA_SCHEMA);
        outJsonData.put("userId", jsonData.getUserId());
        outJsonData.put("id", jsonData.getId());
        outJsonData.put("title", jsonData.getTitle());
        outJsonData.put("body", jsonData.getBody());
        return outJsonData;
    }



    public Integer getSavedOffset(OffsetStorageReader offsetStorageReader){
        if(offsetStorageReader == null){
            return 0;
        }
        Map<String,Object> sourcePartitionValue = sourcePartition();
        Map<String,Object> savedOffset =offsetStorageReader.offset(sourcePartitionValue);

        if(savedOffset == null){
            return 0;
        }

        Integer offsetValue = (Integer) savedOffset.getOrDefault("id", 0);
        log.info("Previous Saved Offset: ", offsetValue);
        return offsetValue;
    }

    public SourceRecord createSourceRecord(JsonData jsonData) {
        return new SourceRecord(sourcePartition(),
                sourceOffset(jsonData),
                topicValue(),
                DATA_SCHEMA,
                createStruct(jsonData));
    }
}
