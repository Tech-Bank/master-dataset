package edu.prz.techbank.masterdataset.parquet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/parquet/scanned-documents")
@RequiredArgsConstructor
@Slf4j
public class ScannedDocumentController {

  final ScannedDocumentService scannedDocumentService;

  @PostMapping
  public ResponseEntity<String> writeScannedDocument(
      @ModelAttribute ScannedDocumentRequest request) {

    log.info("Adding document scan: {}", request);

    scannedDocumentService.writeDocument(
        request.directory,
        request.id,
        request.transactionId,
        request.description,
        request.timestamp,
        request.file,
        request.compressionCodec);

    return ResponseEntity.ok().build();
  }

  public record ScannedDocumentRequest(
      String directory,
      String id,
      String transactionId,
      String description,
      Long timestamp,
      MultipartFile file,
      CompressionCodecName compressionCodec) {

  }
}
