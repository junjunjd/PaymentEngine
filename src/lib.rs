use csv::Error;
use csv::StringRecord;
use log::{debug, error, info, warn};
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;

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
    // Once a transaction's been chargebacked, no further dispute/resolve/chargeback can be made against the transaction.
}

#[derive(PartialEq, Eq, Serialize, Debug)]
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

#[derive(PartialEq, Eq, Serialize, Debug)]
pub struct Account {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
    #[serde(skip_serializing)]
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

    pub fn deposit(&mut self, data: &Transaction) {
        match data.amount {
            Some(value)  => {
                if self.locked {
                    info!(
                        "Transaction #{}: Client #{}. Account is locked. Deposit failed.",
                        data.tx, data.client
                        );
                } else if value < Decimal::ZERO {
                    warn!(
                        "Transaction #{}: Client #{}. Deposit amount is not positive. Transaction is ignored.",
                        data.tx, data.client
                        );
                } else {
                    let mut deposit_amount: Decimal = value;
                    deposit_amount.rescale(4);

                    // Since held will always be non-negative, total will always be
                    // greater than or equal to available. Thus, if adding the deposit amount
                    // to total doesn't cause overflow, adding the same amount to
                    // available will not cause overflow.
                    match self.total.checked_add(deposit_amount) {
                        Some(result) => {
                            self.total = result;
                            self.available = self.available + deposit_amount;

                            // According to the specification, transaction IDs are globally unique. If the same deposit
                            // transaction ID appears more than once, the deposited hashmap will only keep the transaction that appeared most
                            // recently.
                            self.deposited.insert(data.tx, Deposit::new(deposit_amount));
                        },
                        None => error!(
                            "Transaction #{}: Client #{}. Total amount overflowed. Deposit is not processed.",
                            data.tx, data.client
                            ),
                    }
                }
            },
            None => warn!(
                "transaction #{}: Client #{}. Deposit amount is not a valid Decimal number. Transaction is ignored.",
                data.tx, data.client
                ),
        }
    }

    pub fn withdrawl(&mut self, data: &Transaction) {
        match data.amount {
            Some(value) => {
                if self.locked {
                    info!(
                        "Transaction #{}: Client #{}. Account is locked. Withdrawl failed.",
                        data.tx, data.client
                        );
                } else if value < Decimal::ZERO {
                    warn!(
                        "Transaction #{}: Client #{}. Withdrawl amount is not positive. Transaction is ignored.",
                        data.tx, data.client
                        );
                } else {
                    let mut withdrawl_amount: Decimal = value;
                    withdrawl_amount.rescale(4);

                    if self.available < withdrawl_amount {
                        info!(

                            "Transaction #{}: Client #{}. No sufficient available funds. Withdrawl failed.",
                            data.tx, data.client
                            );
                    } else {
                        self.available = self.available - withdrawl_amount;
                        self.total = self.total - withdrawl_amount;
                    }
                }
            },
            None => warn!(
                "transaction #{}: Client #{}. Withdrawl amount is not a valid Decimal number. Transaction is ignored.",
                data.tx, data.client
                ),
        }
    }

    pub fn dispute(&mut self, data: &Transaction) {
        match self.deposited.get_mut(&data.tx) {
            Some(deposited) => {
                match deposited.state {
                    DepositState::Chargebacked =>
                        // Check if the tx has been chargebacked. Once a tx's been chargebacked and reversed, no dispute can be made to the tx.
                        debug!(
                            "Transaction #{}: Client #{}. Transaction has already been chargebacked. This dispute request is ignored. ",
                            data.tx, data.client
                            ),
                    DepositState::Disputed =>
                        // Check if the tx is already under dispute. If so, ignore this dispute.
                        debug!(
                            "Transaction #{}: Client #{}. Transaction is already under dispute. This dispute request is ignored",
                            data.tx, data.client
                            ),
                    DepositState::NotDisputed => {
                        self.available = self.available - deposited.amount;
                        self.held = self.held + deposited.amount;
                        deposited.state = DepositState::Disputed;
                    },
                }
            },

            None => warn!(
                "transaction #{}: Client #{}. Cannot find the deposit transaction related to this dispute. Either the tx specified by the dispute doesn't exist or the specified tx is not a deposit. This dispute request is ignored.",
                data.tx, data.client),
        }
    }

    pub fn resolve(&mut self, data: &Transaction) {
        match self.deposited.get_mut(&data.tx) {
            Some(deposited) => {
                match deposited.state {
                    DepositState::Chargebacked =>
                        // Check if the tx has been chargebacked. Once a tx's been chargebacked and reversed, no dispute can be made to the tx.
                        debug!(
                            "Transaction #{}: Client #{}. Transaction has already been chargebacked. This resolve request is ignored. ",
                            data.tx, data.client
                            ),
                    DepositState::Disputed => {
                        // check if the tx is under dispute. If not, ignore the resolve.
                        self.available = self.available + deposited.amount;
                        self.held = self.held - deposited.amount;
                        // Dispute is considered resolved. The state field now updated to NotDisputed.
                        // The engine assumes that a client can dispute a transaction that's already
                        // been disputed and resolved.
                        deposited.state = DepositState::NotDisputed;
                    },
                    DepositState::NotDisputed =>
                        debug!(
                            "Transaction #{}: Client #{}. Transaction is not under dispute. This resolve request is ignored.",
                            data.tx, data.client
                            ),
                }
            },
            None => warn!(
                "transaction #{}: Client #{}. Cannot find the deposit transaction related to this resolve. Either the tx specified by the resolve doesn't exist or the specified tx is not a deposit. This resolve request is ignored.",
                data.tx, data.client),
        }
    }

    pub fn chargeback(&mut self, data: &Transaction) {
        match self.deposited.get_mut(&data.tx) {
            Some(deposited) => {
                match deposited.state {
                    DepositState::Chargebacked =>
                        // Check if the tx has been chargebacked. Once a tx's been chargebacked and reversed, no dispute can be made to the tx.
                        debug!(
                            "Transaction #{}: Client #{}. Transaction has already been chargebacked. This chargeback request is ignored. ",
                            data.tx, data.client
                            ),
                    DepositState::Disputed => {
                        // check if the tx is under dispute. If not, ignore the chargeback.
                        self.held = self.held - deposited.amount;
                        self.total = self.total - deposited.amount;
                        // A chargeback is the final state of a dispute. The state field now updated to Chargebacked.
                        deposited.state = DepositState::Chargebacked;

                        // Once a chargeback occurs, the client's account should be immediately frozen.
                        self.locked = true;
                    },
                    DepositState::NotDisputed =>
                        debug!(
                            "Transaction #{}: Client #{}. Transaction is not under dispute. This chargeback request is ignored.",
                            data.tx, data.client
                            ),
                }
            },
            None => warn!(
                "transaction #{}: Client #{}. Cannot find the deposit transaction related to this chargeback. Either the tx specified by the chargeback doesn't exist or the specified tx is not a deposit. This chargeback request is ignored.",
                data.tx, data.client),
        }
    }

    pub fn update(&mut self, data: &Transaction) {
        match data.r#type.as_str().trim() {
            "deposit" => self.deposit(data),
            "withdrawl" => self.withdrawl(data),
            "dispute" => self.dispute(data),
            "resolve" => self.resolve(data),
            "chargeback" => self.chargeback(data),
            _ => warn!(
                "transaction #{}: Client #{}. Transaction type is not specified. This transaction is ignored.",
                data.tx, data.client
                ),
        }
    }
}

pub fn process_records<R: io::Read>(rdr: R) -> Result<HashMap<u16, Account>, Error> {
    let mut reader = csv::Reader::from_reader(rdr);

    // trim whitespaces in headers
    let headers = reader.headers()?;
    let mut headers_trimmed = StringRecord::new();
    for i in headers {
        headers_trimmed.push_field(i.trim());
    }
    reader.set_headers(headers_trimmed);

    let mut account_map: HashMap<u16, Account> = HashMap::new();
    for record in reader.deserialize() {
        let transaction: Transaction = record?;
        match account_map.get_mut(&transaction.client) {
            Some(value) => {
                value.update(&transaction);
            }
            None => {
                // Transactions reference clients. If a client doesn't exist create a new account record.
                let mut accountnew = Account::new(transaction.client);
                accountnew.update(&transaction);
                account_map.insert(transaction.client, accountnew);
            }
        }
    }

    Ok(account_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;
    use std::fs::File;

    #[test]
    fn test_deposit() -> Result<(), Error> {
        let test_file_path = "test_data1.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client65535 = Account {
            client: 65535,
            available: dec!(1000000000000.0000),
            held: Decimal::ZERO,
            total: dec!(1000000000000.0000),
            locked: false,
            deposited: HashMap::from([
                (
                    4294967292,
                    Deposit {
                        amount: dec!(999999999999.9999),
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

        assert_eq!(*test_accounts.get(&65535).unwrap(), client65535);
        Ok(())
    }

    #[test]
    fn test_withdrawl() -> Result<(), Error> {
        let test_file_path = "test_data2.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client65535 = Account {
            client: 65535,
            available: dec!(999999999999.9999),
            held: Decimal::ZERO,
            total: dec!(999999999999.9999),
            locked: false,
            deposited: HashMap::from([
                (
                    4294967292,
                    Deposit {
                        amount: dec!(999999999999.9999),
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
            available: dec!(999999999999.9999),
            held: dec!(0),
            total: dec!(999999999999.9999),
            locked: false,
            deposited: HashMap::from([(
                4294967291,
                Deposit {
                    amount: dec!(1000000000000.0000),
                    state: DepositState::NotDisputed,
                },
            )]),
        };

        assert_eq!(*test_accounts.get(&65535).unwrap(), client65535);
        assert_eq!(*test_accounts.get(&65534).unwrap(), client65534);
        Ok(())
    }

    #[test]
    fn test_process_records() -> Result<(), Error> {
        let test_file_path = "test_data3.csv";
        let test_rdr = File::open(test_file_path)?;
        let test_accounts = process_records(test_rdr)?;
        let client1 = Account {
            client: 1,
            available: dec!(-1.5),
            held: dec!(0),
            total: dec!(-1.5),
            locked: true,
            deposited: HashMap::from([
                (
                    1,
                    Deposit {
                        amount: dec!(1),
                        state: DepositState::Chargebacked,
                    },
                ),
                (
                    3,
                    Deposit {
                        amount: dec!(2),
                        state: DepositState::Chargebacked,
                    },
                ),
            ]),
        };
        let client2 = Account {
            client: 2,
            available: dec!(0),
            held: dec!(0),
            total: dec!(0),
            locked: true,
            deposited: HashMap::from([(
                2,
                Deposit {
                    amount: dec!(2),
                    state: DepositState::Chargebacked,
                },
            )]),
        };
        let client3 = Account {
            client: 3,
            available: dec!(1000),
            held: dec!(0),
            total: dec!(1000),
            locked: false,
            deposited: HashMap::from([(
                8,
                Deposit {
                    amount: dec!(1000),
                    state: DepositState::NotDisputed,
                },
            )]),
        };

        assert_eq!(*test_accounts.get(&1).unwrap(), client1);
        assert_eq!(*test_accounts.get(&2).unwrap(), client2);
        assert_eq!(*test_accounts.get(&3).unwrap(), client3);
        Ok(())
    }
}
