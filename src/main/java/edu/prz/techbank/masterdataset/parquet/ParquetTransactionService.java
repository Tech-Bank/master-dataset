package edu.prz.techbank.masterdataset.parquet;

import edu.prz.techbank.masterdataset.domain.Transaction;
import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils.FileType;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetTransactionService {

  final org.apache.hadoop.conf.Configuration hadoopConfig;
  final FileSystem fileSystem;

  @Value("${parquet.max-records}")
  private int maxRecords;

  @Value("${parquet.compression-codec}")
  CompressionCodecName defaultCompressionCodec;

  private List<Transaction> buffer = createBuffer();

  static final String TRANSACTION_SCHEMA = """
      message transaction {
        required binary id (UTF8);
        required binary sender (UTF8);
        required binary beneficiary (UTF8);
        required double amount;
        required binary currency (UTF8);
        required binary date (UTF8);
        required int64 timestamp;
      }
      """;

  static final MessageType schema = MessageTypeParser.parseMessageType(TRANSACTION_SCHEMA);
  static final SimpleGroupFactory factory = new SimpleGroupFactory(schema);

  public void writeTransactions(String directory, LocalDate date, List<Transaction> transactions) {

    Path hdfsPath = HdfsFileUtils.generateNewFilePath(directory, date, FileType.PARQUET);

    log.info("Writing transactions to {}", hdfsPath);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(
            HadoopOutputFile.fromPath(hdfsPath, hadoopConfig))
        .withConf(hadoopConfig)
        .withType(schema)
        .withCompressionCodec(defaultCompressionCodec)
        .build()) {

      for (val t : transactions) {

        writer.write(factory.newGroup()
            .append("id", t.getId())
            .append("sender", t.getSender())
            .append("beneficiary", t.getBeneficiary())
            .append("amount", t.getAmount().doubleValue())
            .append("currency", t.getCurrency())
//            .append("date", t.getDate().toString())
            .append("timestamp", t.getTimestamp()));
      }
    } catch (IOException e) {
      throw new GeneralModuleException("Transactions writing error", e);
    }
  }

  public void writeTransactionsUsingBuffer(String directory, LocalDate date,
      List<Transaction> transactions) {

    if (buffer.size() + transactions.size() > maxRecords) {
      log.info("Max records reached. Doing rotation.");
      val bufferToWrite = buffer;
      this.buffer = createBuffer();
      writeTransactions(directory, date, bufferToWrite);
    }

    buffer.addAll(transactions);
  }

  private List<Transaction> createBuffer() {
    return Collections.synchronizedList(new ArrayList<>());
  }

  public List<Transaction> readTransactionsFromFile(Path hdfsPath) {

    log.info("Read transactions from {}", hdfsPath);

    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hdfsPath)
        .withConf(hadoopConfig)
        .build()) {

      List<Transaction> transactions = new ArrayList<>();

      GenericRecord g;
      while ((g = reader.read()) != null) {
        transactions.add(new Transaction(
            g.get("id").toString(),
            g.get("sender").toString(),
            g.get("beneficiary").toString(),
            (Double) g.get("amount"),
            g.get("currency").toString(),
            (Long) g.get("timestamp")));
      }

      return transactions;

    } catch (IOException e) {
      throw new GeneralModuleException("Transactions writing error", e);
    }
  }

  public List<Transaction> readTransactionsFromDirectory(String directory) {

    Path hdfsPath = new Path(directory);

    List<Transaction> transactions = new ArrayList<>();
    try {
      val status = fileSystem.listStatus(hdfsPath);
      logFiles(status);
      Arrays.stream(status).forEach(s -> {
        if (s.isFile()) {
          transactions.addAll(readTransactionsFromFile(s.getPath()));
        }
      });
    } catch (IOException e) {
      throw new GeneralModuleException("Transactions reading error", e);
    }
    return transactions;
  }

  private void logFiles(FileStatus[] files) {
    val fileNames = Arrays.stream(files)
        .filter(FileStatus::isFile)
        .map(FileStatus::getPath)
        .map(Path::toString)
        .collect(Collectors.joining(", "));
    log.info("Read transactions from files: {}", fileNames);
  }

}
