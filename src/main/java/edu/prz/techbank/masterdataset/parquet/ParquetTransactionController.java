package edu.prz.techbank.masterdataset.parquet;

import edu.prz.techbank.masterdataset.domain.Transaction;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/parquet/transactions")
@RequiredArgsConstructor
public class ParquetTransactionController {

  final ParquetTransactionService transactionService;

  @PostMapping
  public ResponseEntity<Void> writeTransactions(@RequestParam String path,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactions(path, transactions);

    return ResponseEntity.ok().build();
  }

  @PostMapping("/buffered")
  public ResponseEntity<Void> writeTransactionsUsingBuffer(@RequestParam String path,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactionsUsingBuffer(path, transactions);

    return ResponseEntity.ok().build();
  }

}
