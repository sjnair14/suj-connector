import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.sujith.ConfigurationSource;
import org.sujith.JsonData;
import org.sujith.RecordSchemaCreator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class VerifyTest {

    @Test
    public void getJson() throws FileNotFoundException {
        File file = new File("./src/test/resources/value.json");
        JsonData jsonData = null;
        Reader reader = new FileReader(file);
        Gson jsonParser = new Gson();
        JsonArray jsonArray = new JsonParser().parse(reader).getAsJsonArray();
        for(int i=0; i<jsonArray.size(); i++){
            jsonData = jsonParser.fromJson(jsonArray.get(i), JsonData.class);
        }

        AbstractConfig abstractConfig = new AbstractConfig(ConfigurationSource.CONFIG_DEF, Collections.EMPTY_MAP);
        RecordSchemaCreator recordSchemaCreator = new RecordSchemaCreator(abstractConfig);
        SourceRecord output = recordSchemaCreator.createSourceRecord(jsonData);

        Struct outputStruct = (Struct) output.value();
        assertEquals(1, outputStruct.get("userId"));
    }
}
