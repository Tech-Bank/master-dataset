package edu.prz.techbank.masterdataset.hdfs;

import java.util.UUID;
import lombok.AllArgsConstructor;
import org.apache.hadoop.fs.Path;

public class HdfsFileUtils {

  public static Path generateNewFilePath(String directory, FileType fileType) {
    return new Path(directory + "/" + UUID.randomUUID() + "." + fileType.extension);
  }

  @AllArgsConstructor
  public enum FileType {
    AVRO("avro"),
    PARQUET("parquet");

    final String extension;
  }

}
