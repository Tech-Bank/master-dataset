package edu.prz.techbank.masterdataset.parquet;

import edu.prz.techbank.masterdataset.domain.Transaction;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/parquet/transactions")
@RequiredArgsConstructor
public class ParquetTransactionController {

  final ParquetTransactionService transactionService;

  @PostMapping
  public ResponseEntity<Void> writeTransactions(@RequestParam String directory,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactions(directory, transactions);

    return ResponseEntity.ok().build();
  }

  @PostMapping("/buffered")
  public ResponseEntity<Void> writeTransactionsUsingBuffer(@RequestParam String directory,
      @RequestBody List<Transaction> transactions) {

    transactionService.writeTransactionsUsingBuffer(directory, transactions);

    return ResponseEntity.ok().build();
  }

}
