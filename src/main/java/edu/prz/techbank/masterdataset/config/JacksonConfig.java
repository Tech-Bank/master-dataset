package edu.prz.techbank.masterdataset.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.val;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

  abstract static class IgnoreSchemaProperty {

    @JsonIgnore
    abstract void getSchema();
  }

  @Bean
  public ObjectMapper objectMapper() {
    val objectMapper = new ObjectMapper();
    objectMapper.addMixIn(Object.class, IgnoreSchemaProperty.class);
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return objectMapper;
  }
}
