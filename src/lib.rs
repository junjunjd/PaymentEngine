use csv::{ReaderBuilder, StringRecord, Trim};
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("csv parse error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("Failed to parse string into integer: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Decimal parsing error: {0}")]
    DecimalError(#[from] rust_decimal::Error),

    #[error("Missing column `amount`")]
    MissingColumnAmount,

    #[error("Missing column `tx`")]
    MissingColumnTx,

    #[error("Missing column `client`")]
    MissingColumnClient,

    #[error("Missing column `type`")]
    MissingColumnType,

    #[error("Duplicate column `amount`")]
    DuplicateColumnAmount,

    #[error("Duplicate column `tx`")]
    DuplicateColumnTx,

    #[error("Duplicate column `client`")]
    DuplicateColumnClient,

    #[error("Duplicate column `type`")]
    DuplicateColumnType,
}

pub struct ColumnIndex {
    r#type: usize,
    client: usize,
    tx: usize,
    amount: usize,
}

impl ColumnIndex {
    pub fn new() -> Self {
        Self {
            // if the csv data has a column named "type", this field will be updated to the index of "type" column
            r#type: usize::MAX,
            client: usize::MAX,
            tx: usize::MAX,
            amount: usize::MAX,
        }
    }

    pub fn check_missing(&self) -> Result<(), EngineError> {
        if self.amount == usize::MAX {
            return Err(EngineError::MissingColumnAmount);
        }
        if self.tx == usize::MAX {
            return Err(EngineError::MissingColumnTx);
        }
        if self.client == usize::MAX {
            return Err(EngineError::MissingColumnClient);
        }
        if self.r#type == usize::MAX {
            return Err(EngineError::MissingColumnType);
        }
        Ok(())
    }

    pub fn check_duplicate_amount(&self) -> Result<(), EngineError> {
        if self.amount != usize::MAX {
            return Err(EngineError::DuplicateColumnAmount);
        }
        Ok(())
    }

    pub fn check_duplicate_tx(&self) -> Result<(), EngineError> {
        if self.tx != usize::MAX {
            return Err(EngineError::DuplicateColumnTx);
        }
        Ok(())
    }

    pub fn check_duplicate_client(&self) -> Result<(), EngineError> {
        if self.client != usize::MAX {
            return Err(EngineError::DuplicateColumnClient);
        }
        Ok(())
    }

    pub fn check_duplicate_type(&self) -> Result<(), EngineError> {
        if self.r#type != usize::MAX {
            return Err(EngineError::DuplicateColumnType);
        }
        Ok(())
    }
}

#[derive(Deserialize, Debug)]
pub struct Transaction {
    r#type: String,
    client: u16,
    tx: u32,
    #[serde(deserialize_with = "csv::invalid_option")]
    amount: Option<Decimal>,
}

#[derive(PartialEq, Eq, Serialize, Debug)]
pub enum DepositState {
    NotDisputed,
    Disputed,
    Chargebacked,
    // The engine assumes that a client can dispute a transaction that's already been disputed and resolved.
    // The engine will ignore a dispute when the corresponding transaction is already under dispute.
    // Once a transaction's been chargebacked, no dispute/resolve/chargeback can be made against the transaction.
}

#[derive(PartialEq, Eq, Debug)]
pub struct Deposit {
    amount: Decimal,
    state: DepositState,
}

impl Deposit {
    pub fn new(deposited_amount: Decimal) -> Self {
        Self {
            amount: deposited_amount,
            state: DepositState::NotDisputed,
        }
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct Account {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
    #[serde(skip)]
    deposited: HashMap<u32, Deposit>,
}

impl Account {
    pub fn new(client_num: u16) -> Self {
        Self {
            client: client_num,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
            locked: false,
            deposited: HashMap::new(),
        }
    }

    pub fn deposit(&mut self, data: &Transaction, tx_set: &mut HashSet<u32>) {
        // Transaction IDs are assumed to be globally unique. If a duplicate tx appears, the transaction is ignored.
        // We are making a strong assumption: if a deposit tx has an invalid decimal amount such as an empty string, it is ignored but the tx ID will still be added to tx_set.
        // If there is a subsequent new deposit tx with the same ID and a valid decimal amount, this deposit will be ignored due to duplicate tx ID.
        if tx_set.contains(&data.tx) {
            error!(
                "{:?} Transaction ID is not unique. This transaction is ignored.",
                data
            );
            return;
        }
        tx_set.insert(data.tx);
        if let Some(amount) = data.amount {
            if self.locked {
                info!("{:?} Account is locked. Deposit failed.", data);
                return;
            }
            if amount < Decimal::ZERO {
                warn!(
                    "{:?} Deposit amount is not positive. This transaction is ignored.",
                    data
                );
                return;
            }
            let mut deposit_amount: Decimal = amount;
            // Amount is assumed to have a precision of up to four places.
            // In case the input amount has a scale larger than 4, we rescale the scaling factor to 4.
            deposit_amount.rescale(4);
            if let Some(total_new) = self.total.checked_add(deposit_amount) {
                if let Some(available_new) = self.available.checked_add(deposit_amount) {
                    self.total = total_new;
                    self.available = available_new;
                    self.deposited.insert(data.tx, Deposit::new(deposit_amount));
                    return;
                }
            }
            error!(
                "{:?} Amount would overflow. This deposit is not processed.",
                data
            );
            return;
        }
        warn!(
            "{:?} Deposit amount is not a valid Decimal number. Transaction is ignored.",
            data
        );
    }

    pub fn withdrawl(&mut self, data: &Transaction, tx_set: &mut HashSet<u32>) {
        // Transaction IDs are assumed to be globally unique. If a duplicate tx appears, the transaction is ignored.
        // We are making a strong assumption: if a deposit tx has an invalid decimal amount such as an empty string, it is ignored but the tx ID will still be added to tx_set.
        // If there is a subsequent new deposit tx with the same ID and a valid decimal amount, this deposit will be ignored due to duplicate tx ID.
        if tx_set.contains(&data.tx) {
            error!(
                "{:?} Transaction ID is not unique. This transaction is ignored.",
                data
            );
            return;
        }
        tx_set.insert(data.tx);
        if let Some(amount) = data.amount {
            if self.locked {
                info!("{:?} Account is locked. Withdrawl failed.", data);
                return;
            }
            if amount < Decimal::ZERO {
                warn!(
                    "{:?} Withdrawl amount is not positive. This transaction is ignored.",
                    data
                );
                return;
            }
            let mut withdrawl_amount: Decimal = amount;
            // Amount is assumed to have a precision of up to four places.
            // In case the input amount has a scale larger than 4, we rescale the scaling factor to 4.
            withdrawl_amount.rescale(4);
            if self.available < withdrawl_amount {
                info!(
                    "{:?} Available funds are not sufficient. Withdrawl failed.",
                    data
                );
                return;
            }
            if let Some(total_new) = self.total.checked_sub(withdrawl_amount) {
                if let Some(available_new) = self.available.checked_sub(withdrawl_amount) {
                    // Available and total will only be updated if overflow does not occur in both operations.
                    self.total = total_new;
                    self.available = available_new;
                    return;
                }
            }
            error!(
                "{:?} Amount would overflow. This withdrawl is not processed.",
                data
            );
            return;
        }
        warn!(
            "{:?} Withdrawl amount is not a valid Decimal number. Transaction is ignored.",
            data
        );
    }

    pub fn dispute(&mut self, data: &Transaction) {
        if let Some(deposited) = self.deposited.get_mut(&data.tx) {
            match deposited.state {
                // Check if the tx has been chargebacked. Once a tx's been chargebacked and reversed, no dispute/resolve/chargeback can be made to the tx.
                DepositState::Chargebacked => {
                    debug!("{:?} Transaction has already been chargebacked. This dispute request is ignored. ", data);
                    return;
                }
                // Check if the tx is already under dispute. If so, ignore this dispute.
                DepositState::Disputed => {
                    debug!("{:?} Transaction is already under dispute. This dispute request is ignored. ", data);
                    return;
                }
                DepositState::NotDisputed => {
                    if let Some(available_new) = self.available.checked_sub(deposited.amount) {
                        if let Some(held_new) = self.held.checked_add(deposited.amount) {
                            self.available = available_new;
                            self.held = held_new;
                            deposited.state = DepositState::Disputed;
                            return;
                        }
                    }
                    error!(
                        "{:?} Amount would overflow. This dispute is not processed.",
                        data
                    );
                    return;
                }
            }
        }
        debug!("{:?} Either the tx specified doesn't exist or the specified tx is not a deposit or the specified tx belongs to a different client. This tx is ignored.", data);
    }

    pub fn resolve(&mut self, data: &Transaction) {
        if let Some(deposited) = self.deposited.get_mut(&data.tx) {
            match deposited.state {
                // Check if the tx has been chargebacked. Once a tx's been chargebacked and reversed, no dispute/resolve/chargeback can be made to the tx.
                DepositState::Chargebacked => {
                    debug!(
                        "{:?} Transaction has already been chargebacked. This resolve is ignored. ",
                        data
                    );
                    return;
                }
                // check if the tx is under dispute. If not, ignore the resolve.
                DepositState::Disputed => {
                    if let Some(available_new) = self.available.checked_add(deposited.amount) {
                        if let Some(held_new) = self.held.checked_sub(deposited.amount) {
                            self.available = available_new;
                            self.held = held_new;
                            // Dispute is considered resolved. The state now updated to NotDisputed.
                            // The engine assumes that a client can dispute a transaction that's already been disputed and resolved.
                            deposited.state = DepositState::NotDisputed;
                            return;
                        }
                    }
                    error!(
                        "{:?} Amount would overflow. This resolve is not processed.",
                        data
                    );
                    return;
                }
                DepositState::NotDisputed => {
                    debug!(
                        "{:?} Transaction is not under dispute. This resolve is ignored.",
                        data
                    );
                    return;
                }
            }
        }
        debug!("{:?} Either the tx specified doesn't exist or the specified tx is not a deposit or the specified tx belongs to a different client. This tx is ignored.", data);
    }

    pub fn chargeback(&mut self, data: &Transaction) {
        if let Some(deposited) = self.deposited.get_mut(&data.tx) {
            match deposited.state {
                // Check if the tx has been chargebacked. Once a tx's been chargebacked and reversed, no dispute/resolve/chargeback can be made to the tx.
                DepositState::Chargebacked => {
                    debug!("{:?} Transaction has already been chargebacked. This chargeback request is ignored. ", data
                    );
                    return;
                }
                // check if the tx is under dispute. If not, ignore the chargeback.
                DepositState::Disputed => {
                    if let Some(held_new) = self.held.checked_sub(deposited.amount) {
                        if let Some(total_new) = self.total.checked_sub(deposited.amount) {
                            self.held = held_new;
                            self.total = total_new;
                            // A chargeback is the final state of a dispute. The state now updated to Chargebacked.
                            deposited.state = DepositState::Chargebacked;
                            // Once a chargeback occurs, the client's account should be immediately frozen.
                            self.locked = true;
                            return;
                        }
                    }
                    error!(
                        "{:?} Amount would overflow. This chargeback is not processed.",
                        data
                    );
                    return;
                }
                DepositState::NotDisputed => {
                    debug!("{:?} Transaction is not under dispute. This chargeback request is ignored.", data
                    );
                    return;
                }
            }
        }
        debug!("{:?} Either the tx specified doesn't exist or the specified tx is not a deposit or the specified tx belongs to a different client. This tx is ignored.", data);
    }

    pub fn update(&mut self, data: &Transaction, tx_set: &mut HashSet<u32>) {
        match data.r#type.as_str() {
            "deposit" => self.deposit(data, tx_set),
            "withdrawl" => self.withdrawl(data, tx_set),
            "dispute" => self.dispute(data),
            "resolve" => self.resolve(data),
            "chargeback" => self.chargeback(data),
            _ => warn!(
                "{:?} Transaction type is not specified. This transaction is ignored.",
                data
            ),
        }
    }
}

pub fn process_records<R: io::Read>(rdr: R) -> Result<HashMap<u16, Account>, EngineError> {
    // Remove leading and trailing whitespaces
    let mut reader = ReaderBuilder::new().trim(Trim::All).from_reader(rdr);
    let headers = reader.headers()?;
    let mut headers_trimmed = Vec::new();
    // Remove all whitespaces, including whitespaces within a string.
    for i in headers {
        let mut i_ = i.to_string();
        i_.retain(|c| !c.is_whitespace());
        headers_trimmed.push(i_);
    }

    let mut column_index = ColumnIndex::new();
    for (idx, header) in headers_trimmed.iter().enumerate() {
        match header.as_str() {
            "type" => {
                column_index.check_duplicate_type()?;
                column_index.r#type = idx;
            }
            "client" => {
                column_index.check_duplicate_client()?;
                column_index.client = idx;
            }
            "tx" => {
                column_index.check_duplicate_tx()?;
                column_index.tx = idx;
            }
            "amount" => {
                column_index.check_duplicate_amount()?;
                column_index.amount = idx;
            }
            _ => error!("Unexpected column name: {}", header),
        }
    }
    column_index.check_missing()?; // check if type, client, tx and amount columns do exist in the input csv data

    let mut tx_set: HashSet<u32> = HashSet::new(); // stores all transaction IDs that have appeared so far
    let mut account_map: HashMap<u16, Account> = HashMap::new();
    let mut records = StringRecord::new();
    while reader.read_record(&mut records)? {
        let mut row_trimmed = Vec::new();
        // Remove all whitespaces, including whitespaces within a string.
        for fields in &records {
            let mut fields_ = fields.to_string();
            fields_.retain(|c| !c.is_whitespace());
            row_trimmed.push(fields_);
        }
        let transaction = Transaction {
            r#type: row_trimmed[column_index.r#type].clone(),
            client: row_trimmed[column_index.client].parse::<u16>()?,
            tx: row_trimmed[column_index.tx].parse::<u32>()?,
            amount: Decimal::from_str(row_trimmed[column_index.amount].as_str()).ok(),
        };
        match account_map.get_mut(&transaction.client) {
            Some(account) => account.update(&transaction, &mut tx_set),
            None => {
                // Transactions reference clients. If a client doesn't exist create a new account record.
                let mut accountnew = Account::new(transaction.client);
                accountnew.update(&transaction, &mut tx_set);
                account_map.insert(transaction.client, accountnew);
            }
        }
    }
    Ok(account_map)
}

// Parses output csv file to account hashmap. This function is used for unit tests.
pub fn parse_csv<R: io::Read>(rdr: R) -> Result<HashMap<u16, Account>, EngineError> {
    let mut reader = csv::Reader::from_reader(rdr);
    let mut account_map: HashMap<u16, Account> = HashMap::new();
    for record in reader.deserialize() {
        let account: Account = record?;
        account_map.insert(account.client, account);
    }
    Ok(account_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;
    use std::fs::File;
    use std::io::{BufReader, BufWriter};

    #[test]
    fn test_deposit() -> Result<(), EngineError> {
        let test_file_path = "test_deposit.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client65535 = Account {
            client: 65535,
            available: dec!(10_000_000_000_000.0000),
            held: Decimal::ZERO,
            total: dec!(10_000_000_000_000.0000),
            locked: false,
            deposited: HashMap::from([
                (
                    4294967294,
                    Deposit {
                        amount: dec!(9_999_999_999_999.9999),
                        state: DepositState::NotDisputed,
                    },
                ),
                (
                    4294967295,
                    Deposit {
                        amount: dec!(0.0001),
                        state: DepositState::NotDisputed,
                    },
                ),
            ]),
        };
        assert_eq!(*test_accounts.get(&65535).unwrap(), client65535);
        Ok(())
    }

    #[test]
    fn test_withdrawl() -> Result<(), EngineError> {
        let test_file_path = "test_withdrawl.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client65535 = Account {
            client: 65535,
            available: dec!(9_999_999_999_999.9999),
            held: Decimal::ZERO,
            total: dec!(9_999_999_999_999.9999),
            locked: false,
            deposited: HashMap::from([
                (
                    4294967292,
                    Deposit {
                        amount: dec!(9_999_999_999_999.9999),
                        state: DepositState::NotDisputed,
                    },
                ),
                (
                    4294967293,
                    Deposit {
                        amount: dec!(0.0001),
                        state: DepositState::NotDisputed,
                    },
                ),
            ]),
        };
        let client65534 = Account {
            client: 65534,
            available: dec!(9_999_999_999_999.9999),
            held: Decimal::ZERO,
            total: dec!(9_999_999_999_999.9999),
            locked: false,
            deposited: HashMap::from([(
                4294967291,
                Deposit {
                    amount: dec!(10_000_000_000_000.0000),
                    state: DepositState::NotDisputed,
                },
            )]),
        };
        assert_eq!(*test_accounts.get(&65535).unwrap(), client65535);
        assert_eq!(*test_accounts.get(&65534).unwrap(), client65534);
        Ok(())
    }

    #[test]
    fn test_process_records() -> Result<(), EngineError> {
        let test_file_path = "test_process_records.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client1 = Account {
            client: 1,
            available: dec!(-1.5000),
            held: Decimal::ZERO,
            total: dec!(-1.5000),
            locked: true,
            deposited: HashMap::from([
                (
                    1,
                    Deposit {
                        amount: dec!(1.0000),
                        state: DepositState::Chargebacked,
                    },
                ),
                (
                    3,
                    Deposit {
                        amount: dec!(2.0000),
                        state: DepositState::Chargebacked,
                    },
                ),
            ]),
        };
        let client2 = Account {
            client: 2,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
            locked: true,
            deposited: HashMap::from([(
                2,
                Deposit {
                    amount: dec!(2.0000),
                    state: DepositState::Chargebacked,
                },
            )]),
        };
        let client3 = Account {
            client: 3,
            available: Decimal::ZERO,
            held: dec!(1000.0000),
            total: dec!(1000.0000),
            locked: false,
            deposited: HashMap::from([(
                8,
                Deposit {
                    amount: dec!(1000.0000),
                    state: DepositState::Disputed,
                },
            )]),
        };
        assert_eq!(*test_accounts.get(&1).unwrap(), client1);
        assert_eq!(*test_accounts.get(&2).unwrap(), client2);
        assert_eq!(*test_accounts.get(&3).unwrap(), client3);
        Ok(())
    }

    #[test]
    fn test_output() -> Result<(), EngineError> {
        let test_file_path = "test_process_records.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let output_file_path = "output_test_process_records.csv";
        let mut output_rdr = File::create(output_file_path)?;
        let bufwrt = BufWriter::new(&mut output_rdr);
        let mut writer = csv::Writer::from_writer(bufwrt);
        for (_, val) in &test_accounts {
            writer.serialize(val)?;
        }
        writer.flush()?;
        let rdr = File::open(output_file_path)?;
        let bufrdr = BufReader::new(rdr);
        let output_accounts = parse_csv(bufrdr)?;
        let accounts_true: HashMap<u16, Account> = HashMap::from([
            (
                1,
                Account {
                    client: 1,
                    available: dec!(-1.5000),
                    held: dec!(0.0000),
                    total: dec!(-1.5000),
                    locked: true,
                    deposited: HashMap::new(),
                },
            ),
            (
                2,
                Account {
                    client: 2,
                    available: dec!(0.0000),
                    held: dec!(0.0000),
                    total: dec!(0.0000),
                    locked: true,
                    deposited: HashMap::new(),
                },
            ),
            (
                3,
                Account {
                    client: 3,
                    available: dec!(0.0000),
                    held: dec!(1000.0000),
                    total: dec!(1000.0000),
                    locked: false,
                    deposited: HashMap::new(),
                },
            ),
        ]);
        assert_eq!(output_accounts, accounts_true);
        Ok(())
    }

    #[test]
    fn test_whitespaces() -> Result<(), EngineError> {
        let test_file_path = "test_whitespaces.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client10 = Account {
            client: 10,
            available: dec!(1.0000),
            held: Decimal::ZERO,
            total: dec!(1.0000),
            locked: false,
            deposited: HashMap::from([(
                100,
                Deposit {
                    amount: dec!(1.0000),
                    state: DepositState::NotDisputed,
                },
            )]),
        };
        let client20 = Account {
            client: 20,
            available: dec!(2.0000),
            held: Decimal::ZERO,
            total: dec!(2.0000),
            locked: false,
            deposited: HashMap::from([(
                200,
                Deposit {
                    amount: dec!(2.0000),
                    state: DepositState::NotDisputed,
                },
            )]),
        };
        assert_eq!(*test_accounts.get(&10).unwrap(), client10);
        assert_eq!(*test_accounts.get(&20).unwrap(), client20);
        Ok(())
    }

    #[test]
    fn test_columns() -> Result<(), EngineError> {
        let test_file_path = "test_columns.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client10 = Account {
            client: 10,
            available: dec!(1.0000),
            held: Decimal::ZERO,
            total: dec!(1.0000),
            locked: false,
            deposited: HashMap::from([(
                100,
                Deposit {
                    amount: dec!(1.0000),
                    state: DepositState::NotDisputed,
                },
            )]),
        };
        let client20 = Account {
            client: 20,
            available: dec!(2.0000),
            held: Decimal::ZERO,
            total: dec!(2.0000),
            locked: false,
            deposited: HashMap::from([(
                200,
                Deposit {
                    amount: dec!(2.0000),
                    state: DepositState::NotDisputed,
                },
            )]),
        };
        assert_eq!(*test_accounts.get(&10).unwrap(), client10);
        assert_eq!(*test_accounts.get(&20).unwrap(), client20);
        Ok(())
    }
}
