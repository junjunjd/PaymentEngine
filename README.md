# PaymentEngine

PaymentEngine is a toy engine written in pure Rust that reads transactions from a CSV, updates client accounts, handles disputes and chargebacks, and then outputs the state of clients accounts as a CSV.

CSV rows are streamed through structs that implement the Read trait without loading the entire data set upfront in memory. This means that the `process_records` function that processes the transaction data is agnostic to concrete data sources which can be CSV files or TCP streams.

There is no use of unsafe code so Rust complier gaurantees type safety. CSV rows are parsed into `Transaction` structs. The type of each field in the `Transaction` struct is defined according to the assumptions of the input data.

Errors are return to the caller of `process_records` function.

## Assumptions
### Input
The input is a CSV file with the columns type, client, tx, and amount, where the type is a string, the client column is a valid u16 client ID, the tx is a valid u32 transaction ID, and the amount is a decimal value with a precision of up to four places past the decimal. 
<br />
<br />
All whitespaces within a string are accepted by the engine, including leading and trailing whitespaces and whitespaces appeared in a string.

### Decimal amount
The engine uses the Decimal type defined by the crate rust-decimal. 
<br />
<br />
If the input decimal amount has a scale larger than 4, the engine will rescale the scaling factor to 4 using the MidpointAwayFromZero strategy. 

### Transaction ID
Transaction IDs (tx) are globally unique and transactions occur chronologically in the input file. 
<br />
<br />
The engine uses a HashSet to keep track of transaction IDs that has already been processed. If a transaction ID already exist in the processed tx HashSet, the transaction is ignored. 
<br />
<br />
Transactions ignored and not processed are not added to the HashSet. Thus, if a tx contains invalid decimal amount and thus is ignored, the tx ID is not added to the HashSet. A new tx with the same ID and a valid decimal amount will still be processed.
<br />
<br />
When a disbute, resolve or chargeback occurs, the engine will only search for the corresponding ID occured in previous transactions.

### Locked account
Once an account's been locked, no deposit or withdrawl can be made to the account.

### Dispute, resolve and chargeback

The payment engine assumes that the dispute, resolve and chargeback are all sent from credit card issuers. Therefore,
- a dispute will only reference a deposit transaction. From the perspective of a credit card issuer, it does not make much sense to dispute a money that has already been credited to the card. Thus, when handling dispute, the engine will only search for the specified tx in previous deposit transactions.

- the engine assumes that a client can dispute a transaction that has already been disputed and resolved. The engine will ignore a dispute when the corresponding transaction is already under dispute. Once a transaction has been chargebacked, no dispute/resolve/chargeback can be made against the transaction.

- a dispute, resolve or chargeback can occur after an account has been locked. Suppose that a dispute has been made against a locked account. The tx specified by the dipute had happened before the account has been locked. The engine will process the dispute the same way it will do to an unlocked account.

### Output
The engine outputs available amounts, held amounts and total amounts with a precision of four places past the decimal. 
## Getting Started
The CLI `payment_engine` takes one arguments to run: the input CSV file path.
```sh
cargo run -- transactions.csv
```
Output will be written to std out.

To execute unit test, run:
```sh
cargo test
```
The unit test will load prepared test data from `test_data1.csv`, `test_data2.csv` and `test_data3.csv`.

To use `payment_engine` as a library, see the example code below:
```rust
use payment_engine::process_records;
use std::fs::File;
use std::io;
use std::io::BufReader;

let rdr = File::open("transactions.csv")?;
let bufrdr = BufReader::new(rdr);
let accounts = process_records(bufrdr)?;
for (_, account) in &accounts {
    println!("{:?}", account);
}
```
