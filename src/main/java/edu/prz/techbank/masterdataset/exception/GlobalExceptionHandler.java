package edu.prz.techbank.masterdataset.exception;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

  @ExceptionHandler(Exception.class)
  public ResponseEntity<Object> handleAny(Exception e, WebRequest request) {

    val description = "Internal server error";

    val problemResponse = ProblemResponse.of(
        "internalServerError",
        description,
        description,
        HttpStatus.INTERNAL_SERVER_ERROR.value(),
        e.getMessage(),
        null);

    log.error(description, e);

    return handleExceptionInternal(e, problemResponse, new HttpHeaders(),
        HttpStatus.INTERNAL_SERVER_ERROR, request);
  }

}