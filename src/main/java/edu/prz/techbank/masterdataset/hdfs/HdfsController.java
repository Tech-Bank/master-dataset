package edu.prz.techbank.masterdataset.hdfs;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletResponse;
import java.util.List;

@RestController
@RequestMapping("/hdfs")
public class HdfsController {

  private final HdfsService hdfsService;

  public HdfsController(HdfsService hdfsService) {
    this.hdfsService = hdfsService;
  }

  @PostMapping("/upload")
  public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file,
      @RequestParam("path") String path) {
    try {
      hdfsService.uploadFile(path, file);
      return ResponseEntity.ok("Plik zapisany w HDFS: " + path);
    } catch (Exception e) {
      return ResponseEntity.internalServerError().body("Błąd: " + e.getMessage());
    }
  }

  @GetMapping("/download")
  public void downloadFile(@RequestParam("path") String path, HttpServletResponse response) {
    try {
      response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + path + "\"");
      response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

      hdfsService.downloadFile(path, response.getOutputStream());
      response.flushBuffer();
    } catch (Exception e) {
      throw new RuntimeException("Błąd pobierania pliku: " + e.getMessage());
    }
  }

  @GetMapping("/list")
  public ResponseEntity<List<String>> listFiles(@RequestParam("directory") String directory) {
    try {
      List<String> files = hdfsService.listFiles(directory);
      return ResponseEntity.ok(files);
    } catch (Exception e) {
      return ResponseEntity.internalServerError().build();
    }
  }

  @DeleteMapping("/delete")
  public ResponseEntity<String> deleteFile(@RequestParam("path") String path) {
    try {
      boolean deleted = hdfsService.deleteFile(path);
      if (deleted) {
        return ResponseEntity.ok("Usunięto: " + path);
      } else {
        return ResponseEntity.status(404).body("Nie znaleziono pliku: " + path);
      }
    } catch (Exception e) {
      return ResponseEntity.internalServerError().body("Błąd: " + e.getMessage());
    }
  }
}
