package edu.prz.techbank.masterdataset.avro;

import edu.prz.techbank.masterdataset.domain.avro.Transaction;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/avro/transactions")
@RequiredArgsConstructor
@Slf4j
public class AvroTransactionController {

  final AvroTransactionService transactionService;

  @PostMapping
  public ResponseEntity<Void> writeTransactions(@RequestParam String directory,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactions(directory, transactions);

    return ResponseEntity.ok().build();
  }

  @GetMapping
  public ResponseEntity<List<Transaction>> readTransactions(@RequestParam String path) {

    List<Transaction> transactions = transactionService.readTransactionsFromFile(path);

    log.info("Number of transactions read: {}. File: {}", transactions.size(), path);

    return ResponseEntity.ok(transactions);
  }

  @GetMapping("/all")
  public ResponseEntity<List<Transaction>> readTransactionsFromDirectory(@RequestParam String directory) {

    List<Transaction> transactions = transactionService.readTransactionsFromDirectory(directory);

    log.info("Number of transactions read: {}. Directory: {}", transactions.size(), directory);

    return ResponseEntity.ok(transactions);
  }
}
