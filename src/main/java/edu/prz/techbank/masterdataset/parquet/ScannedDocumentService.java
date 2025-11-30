package edu.prz.techbank.masterdataset.parquet;

import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils.FileType;
import java.io.IOException;
import java.io.InputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScannedDocumentService {

  final org.apache.hadoop.conf.Configuration hadoopConfig;

  @Value("${parquet.compression-codec}")
  CompressionCodecName defaultCompressionCodec;

  static final String DOCUMENT_SCHEMA = """
      message scanned_document {
        required binary id (UTF8);
        required binary transactionId (UTF8);
        optional binary description (UTF8);
        required int64 timestamp;
        required binary scan;
      }
      """;

  static final MessageType schema = MessageTypeParser.parseMessageType(DOCUMENT_SCHEMA);
  static final SimpleGroupFactory factory = new SimpleGroupFactory(schema);

  public void writeDocument(String directory,
      String id, String transactionId, String description, Long timestamp,
      MultipartFile file, CompressionCodecName compressionCodec) {

    Path hdfsPath = HdfsFileUtils.generateNewFilePath(directory, FileType.PARQUET);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(
            HadoopOutputFile.fromPath(hdfsPath, hadoopConfig))
        .withConf(hadoopConfig)
        .withType(schema)
        .withCompressionCodec(compressionCodec == null ? defaultCompressionCodec : compressionCodec)
        .build();
        InputStream inputStream = file.getInputStream()) {

      val scan = inputStream.readAllBytes();

      writer.write(factory.newGroup()
          .append("id", id)
          .append("transactionId", transactionId)
          .append("description", description)
          .append("timestamp", timestamp)
          .append("scan", Binary.fromConstantByteArray(scan)));

    } catch (IOException e) {
      throw new GeneralModuleException("Document writing error", e);
    }
  }

}
