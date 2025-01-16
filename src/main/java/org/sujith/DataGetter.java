package org.sujith;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class DataGetter extends TimerTask {

    private final SortedSet<JsonData> dataRecordsSet;

    private static final String URL_VAL = "https://jsonplaceholder.typicode.com/posts";

    private URL urlObject;

    private final Gson jsonParser = new Gson();

    private int dataOffset;

    public DataGetter(AbstractConfig abstractConfig, int offset) throws MalformedURLException {

        dataRecordsSet = new TreeSet<>(new JsonDataComparator());
        urlObject =  URI.create(URL_VAL).toURL();
        dataOffset = offset;

    }

    public void run(){
        try{
        URLConnection connection = urlObject.openConnection();
        Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8);

            JsonArray jsonArray = new JsonParser().parse(reader).getAsJsonArray();
            for(int i=0; i<jsonArray.size(); i++) {
                try{
                JsonData jsonData = jsonParser.fromJson(jsonArray.get(i), JsonData.class);
                if (!jsonData.getTitle().isBlank() && !jsonData.getBody().isBlank()) {
                    if (jsonData.getUserId() > dataOffset) {
                        synchronized (this) {
                            dataRecordsSet.add(jsonData);
                        }
                        dataOffset = jsonData.getUserId();
                    } else {
                        log.info("Duplicate Value");
                    }
                } else {
                    log.error("Data is Empty");
                }
            }catch(JsonSyntaxException exception){
                    log.error("Irregular data, unexpected datatype. Record is discarded.");
                }
            }
        }
        catch (IOException exception){
            log.info("Error while accessing URL", exception);
        }
    }

    public synchronized List<JsonData> getData(){
        List<JsonData> jsonDataList = new ArrayList<>();
        while (!dataRecordsSet.isEmpty()){
        JsonData next = dataRecordsSet.first();
        boolean deleted = dataRecordsSet.remove(next);
        if(!deleted){
            log.error("Item not deleted");
        }else {
            jsonDataList.add(next);
        }
        }
        return jsonDataList;
    }


}
