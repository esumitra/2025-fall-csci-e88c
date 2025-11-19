package org.cscie88c.kafka

final case class Transaction(
  number: String,
  cardType: String,
  amount: Double,
  accountNumber: String,
  expiry: String,
  transactionTime: String
)

object Transaction {

  def apply(csvRow: String): Option[Transaction] = {
    val fields = csvRow.split(",").map(_.trim)
    if (fields.length == 6) {
      Some(
        Transaction(
          number = fields(0),
          cardType = fields(1),
          amount = fields(2).toDouble,
          accountNumber = fields(3),
          expiry = fields(4),
          transactionTime = fields(5)
        )
      )
    } else {
      None
    }
  }
}
