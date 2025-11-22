package edu.prz.techbank.masterdataset.exception;

public class GeneralModuleException extends RuntimeException {

  public GeneralModuleException(String message) {
    super(message);
  }

  public GeneralModuleException(String message, Throwable cause) {
    super(message, cause);
  }
}
