package edu.prz.techbank.masterdataset.parquet;

import edu.prz.techbank.masterdataset.domain.avro.Transaction;
import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils.FileType;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetTransactionServiceAvroSchema {

  private static final int TRANSACTIONS_PAGE_SIZE = 1024 * 1024;
  private static final long TRANSACTIONS_GROUP_SIZE = 256 * 1024 * 1024L;

  final org.apache.hadoop.conf.Configuration hadoopConfig;

  @Value("${parquet.max-records}")
  private int maxRecords;

  @Value("${parquet.compression-codec}")
  CompressionCodecName defaultCompressionCodec;

  private Schema schema;

  private List<Transaction> buffer = createBuffer();

  @PostConstruct
  public void init() throws Exception {
    // load Avro schema
    InputStream is = getClass().getClassLoader().getResourceAsStream("Transaction.avsc");
    this.schema = new Schema.Parser().parse(is);
  }

  public void writeTransactions(String directory, List<Transaction> transactions) {

    Path hdfsPath = HdfsFileUtils.generateNewFilePath(directory, FileType.PARQUET);

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hdfsPath)
        .withSchema(schema)
        .withConf(hadoopConfig)
        .withCompressionCodec(defaultCompressionCodec)
        .withPageSize(TRANSACTIONS_PAGE_SIZE)
        .withRowGroupSize(TRANSACTIONS_GROUP_SIZE)
        .build()) {

      for (Transaction dto : transactions) {

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", dto.getId());
        record.put("sender", dto.getSender());
        record.put("beneficiary", dto.getBeneficiary());
        record.put("amount", dto.getAmount());
        record.put("currency", dto.getCurrency());
        record.put("timestamp", dto.getTimestamp());

        writer.write(record);
      }
    } catch (IOException e) {
      throw new GeneralModuleException("Transactions writing error", e);
    }
  }

  public void writeTransactionsUsingBuffer(String directory, List<Transaction> transactions) {

    if (buffer.size() + transactions.size() > maxRecords) {
      log.info("Max records reached. Doing rotation.");
      val bufferToWrite = buffer;
      this.buffer = createBuffer();
      writeTransactions(directory, bufferToWrite);
    }

    buffer.addAll(transactions);
  }

  private List<Transaction> createBuffer() {
    return Collections.synchronizedList(new ArrayList<>());
  }

}
