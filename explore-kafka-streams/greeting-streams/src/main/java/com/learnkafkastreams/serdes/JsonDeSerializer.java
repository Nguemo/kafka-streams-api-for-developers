package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public class JsonDeSerializer<T> implements Deserializer<T> {

    Class<T> destinationClass ;

    public JsonDeSerializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public  ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS,false);


    @Override
    public T deserialize(String topic, byte[] data) {
        if(Objects.isNull(data)) return null ;
        try {
            return objectMapper.readValue(data, destinationClass);
        } catch (IOException e) {
            log.error("IOException in deserialization : {} : ",e.getMessage(),e);
            throw new RuntimeException(e);
        }catch (Exception e){
            log.error("Exception in deserialization : {} : ",e.getMessage(),e);
            throw  new RuntimeException(e);
        }
    }
}
