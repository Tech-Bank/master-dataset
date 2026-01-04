package edu.prz.techbank.masterdataset.parquet;

import edu.prz.techbank.masterdataset.domain.Transaction;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/parquet/transactions")
@RequiredArgsConstructor
public class ParquetTransactionController {

  final ParquetTransactionService transactionService;
  final ParquetTransactionServiceAvroSchema transactionServiceAvroSchema;

  @PostMapping
  public ResponseEntity<Void> writeTransactionsUsingParquetSchema(
      @RequestParam String directory,
      @RequestParam LocalDate date,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactions(directory, date, transactions);

    return ResponseEntity.ok().build();
  }

  @PostMapping("/buffered")
  public ResponseEntity<Void> writeTransactionsUsingParquetSchemaAndBuffer(
      @RequestParam String directory,
      @RequestParam LocalDate date,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactionsUsingBuffer(directory, date, transactions);

    return ResponseEntity.ok().build();
  }

  @GetMapping
  public ResponseEntity<List<Transaction>> readTransactionsUsingParquetSchema(
      @RequestParam String directory,
      @RequestParam LocalDate date) {

    return ResponseEntity.ok(
        transactionService.readTransactionsFromDirectory(directory + "/date=" + date));
  }

  @PostMapping("/avro-schema")
  public ResponseEntity<Void> writeTransactionsUsingAvroSchema(@RequestParam String directory,
      @RequestBody List<edu.prz.techbank.masterdataset.domain.avro.Transaction> transactions) {

    transactionServiceAvroSchema.writeTransactions(directory, transactions);

    return ResponseEntity.ok().build();
  }

  @PostMapping("/avro-schema/buffered")
  public ResponseEntity<Void> writeTransactionsUsingAvroSchemaAndBuffer(
      @RequestParam String directory,
      @RequestBody List<edu.prz.techbank.masterdataset.domain.avro.Transaction> transactions) {

    transactionServiceAvroSchema.writeTransactionsUsingBuffer(directory, transactions);

    return ResponseEntity.ok().build();
  }

}
