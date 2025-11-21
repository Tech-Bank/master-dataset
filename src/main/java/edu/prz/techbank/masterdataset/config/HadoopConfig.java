package edu.prz.techbank.masterdataset.config;

import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class HadoopConfig {

  @Value("${hdfs.uri}")
  String hdfsUri;

  @Bean
  public org.apache.hadoop.conf.Configuration hadoopConfiguration() {
    val config = new org.apache.hadoop.conf.Configuration();
    config.set("fs.defaultFS", hdfsUri);
    log.info("HDFS URI: {}", hdfsUri);
    config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    config.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return config;
  }

  @Bean
  public FileSystem fileSystem(org.apache.hadoop.conf.Configuration config) throws Exception {
    return FileSystem.get(new URI(hdfsUri), config);
  }
}
