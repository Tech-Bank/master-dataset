package edu.prz.techbank.masterdataset.parquet;

import edu.prz.techbank.masterdataset.domain.Transaction;
import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
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
public class ParquetTransactionService {

  final org.apache.hadoop.conf.Configuration hadoopConfiguration;

  @Value("${parquet.max-records}")
  private int maxRecords;

  private Schema schema;

  private List<Transaction> buffer = createBuffer();

  @PostConstruct
  public void init() throws Exception {
    // load Avro schema
    InputStream is = getClass().getClassLoader().getResourceAsStream("Transaction.avsc");
    this.schema = new Schema.Parser().parse(is);
  }

  public void writeTransactions(String path, List<Transaction> transactions) {

    Path hdfsPath = new Path(path);

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hdfsPath)
        .withSchema(schema)
        .withConf(hadoopConfiguration)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withPageSize(1024 * 1024)
        .withRowGroupSize(256 * 1024 * 1024L)
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

  public void writeTransactionsUsingBuffer(String path, List<Transaction> transactions) {

    if (buffer.size() + transactions.size() > maxRecords) {
      log.info("Max records reached. Doing rotation.");
      val bufferToWrite = buffer;
      this.buffer = createBuffer();
      writeTransactions(path + "/" + LocalDateTime.now(), bufferToWrite);
    }

    buffer.addAll(transactions);
  }

  private List<Transaction> createBuffer() {
    return Collections.synchronizedList(new ArrayList<>());
  }

}
