package edu.prz.techbank.masterdataset.avro;

import edu.prz.techbank.masterdataset.domain.Transaction;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {

  final TransactionService transactionService;

  @PostMapping
  public ResponseEntity<Void> writeTransactions(@RequestParam String path,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactions(path, transactions);

    return ResponseEntity.ok().build();
  }

  @GetMapping
  public ResponseEntity<List<Transaction>> readTransactions(@RequestParam String directory) {

    List<Transaction> transactions = transactionService.readTransactionsFromDirectory(directory);

    log.info("Number of transactions read: {}. Path: {}", transactions.size(), directory);

    return ResponseEntity.ok(transactions);
  }
}
