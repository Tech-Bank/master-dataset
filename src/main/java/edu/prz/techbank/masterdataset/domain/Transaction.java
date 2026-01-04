package edu.prz.techbank.masterdataset.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {

  String id;
  String sender;
  String beneficiary;
  Double amount;
  String currency;
  Long timestamp;
}
