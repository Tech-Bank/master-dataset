package edu.prz.techbank.masterdataset.avro;

import edu.prz.techbank.masterdataset.domain.avro.Transaction;
import edu.prz.techbank.masterdataset.exception.GeneralModuleException;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils;
import edu.prz.techbank.masterdataset.hdfs.HdfsFileUtils.FileType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AvroTransactionService {

  final FileSystem fileSystem;

  public void writeTransactions(String directory, List<Transaction> transactions) {

    Path hdfsPath = HdfsFileUtils.generateNewFilePath(directory, FileType.AVRO);

    try (FSDataOutputStream out = fileSystem.create(hdfsPath)) {
      DatumWriter<Transaction> dw = new SpecificDatumWriter<>(Transaction.class);
      try (DataFileWriter<Transaction> dfw = new DataFileWriter<>(dw)) {
        dfw.create(Transaction.getClassSchema(), out);
        for (Transaction t : transactions) {
          dfw.append(t);
        }
      }
    } catch (IOException e) {
      throw new GeneralModuleException("Transactions writing error", e);
    }
  }

  public List<Transaction> readTransactionsFromDirectory(String directory) {

    Path hdfsPath = new Path(directory);

    List<Transaction> transactions = new ArrayList<>();
    try {
      val status = fileSystem.listStatus(hdfsPath);
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

  public List<Transaction> readTransactionsFromFile(String path) {
    Path hdfsPath = new Path(path);
    return readTransactionsFromFile(hdfsPath);
  }

  private List<Transaction> readTransactionsFromFile(Path hdfsPath) {
    List<Transaction> result = new ArrayList<>();

    try (FSDataInputStream in = fileSystem.open(hdfsPath)) {
      DatumReader<Transaction> dr = new SpecificDatumReader<>(Transaction.class);
      try (DataFileReader<Transaction> dfr = new DataFileReader<>(
          new SeekableInput() {
            @Override
            public void seek(long p) throws IOException {
              in.seek(p);
            }

            @Override
            public long tell() throws IOException {
              return in.getPos();
            }

            @Override
            public long length() throws IOException {
              return fileSystem.getFileStatus(hdfsPath).getLen();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
              return in.read(b, off, len);
            }

            @Override
            public void close() throws IOException {
              in.close();
            }
          }, dr)) {
        while (dfr.hasNext()) {
          result.add(dfr.next());
        }
      }
    } catch (IOException e) {
      throw new GeneralModuleException("Transactions reading error", e);
    }

    return result;
  }

}
