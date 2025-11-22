package edu.prz.techbank.masterdataset.exception;

import lombok.Data;

@Data
public class ProblemResponse {

  String type;
  String title;
  String description;
  Integer status;
  String detail;
  String instance;

  private ProblemResponse(String type, String title, String description, Integer status,
      String detail, String instance) {
    this.type = type;
    this.title = title;
    this.description = description;
    this.status = status;
    this.detail = detail;
    this.instance = instance;
  }

  public static ProblemResponse of(String type, String title, String description, Integer status,
      String detail, String instance) {
    return new ProblemResponse(type, title, description, status, detail, instance);
  }
}
