use csv::Error;
use csv::StringRecord;
use log::{debug, info};
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
pub struct Deposit {
    amount: Decimal,
    disputed: bool,
}

impl Deposit {
    pub fn new(deposited_amount: Decimal) -> Self {
        Self {
            amount: deposited_amount,
            disputed: false,
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
                    info!(
                        "Transaction #{}: Client #{}. Deposit amount is not positive. Transaction is ignored.",
                        data.tx, data.client
                        );
                } else {
                    let mut deposit_amount: Decimal = value;
                    if deposit_amount.scale() > 4 {
                        debug!(
                            "Transaction #{}: Client #{}. Deposit amount has a precision more than four places. The amount is rescaled to a precision of up to four places.",
                            data.tx, data.client
                            );
                        deposit_amount.rescale(4);
                    };

                    // Since held will always be non-negative, available will always be
                    // smaller than or equal to total. Thus, if adding the deposit amount
                    // to total doesn't cause overflow, adding the same amount to
                    // available will not cause overflow.
                    match self.total.checked_add(deposit_amount) {
                        Some(result) => {
                            self.total = result;
                            self.available = self.available + deposit_amount;

                            // According to the specification, transaction IDs are globally unique. If the same deposit
                            // transaction ID appears more than once, the engine will keep records of the transaction that appears most
                            // recently.
                            self.deposited.insert(data.tx, Deposit::new(deposit_amount));
                        },
                        None => info!(
                            "Transaction #{}: Client #{}. Total amount overflowed. Deposit is not processed.",
                            data.tx, data.client
                            ),
                    }
                }
            },
            None => info!(
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
                    info!(
                        "Transaction #{}: Client #{}. Withdrawl amount is not positive. Transaction is ignored.",
                        data.tx, data.client
                        );
                } else {
                    let mut withdrawl_amount: Decimal = value;
                    if withdrawl_amount.scale() > 4 {
                        debug!(
                            "Transaction #{}: Client #{}. Withdrawl amount has a precision more than four places. The amount is rescaled to a precision of up to four places.",
                            data.tx, data.client
                            );
                        withdrawl_amount.rescale(4);
                    };

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
            None => info!(
                "transaction #{}: Client #{}. Withdrawl amount is not a valid Decimal number. Transaction is ignored.",
                data.tx, data.client
                ),
        }
    }

    pub fn dispute(&mut self, data: &Transaction) {
        match self.deposited.get_mut(&data.tx) {
            Some(deposited) => {
                if deposited.disputed {
                    debug!(
                        "Transaction #{}: Client #{}. Transaction is already under dispute.",
                        data.tx, data.client
                        );
                } else {
                    self.available = self.available - deposited.amount;
                    self.held = self.held + deposited.amount;
                    deposited.disputed = true;
                }
            },

            None => info!(
                "transaction #{}: Client #{}. Cannot find the deposit transaction related to this dispute. Either the tx specified by the dispute doesn't exist or the specified tx is not a deposit. Dispute is ignored.",
                data.tx, data.client),
        }
    }

    pub fn resolve(&mut self, data: &Transaction) {
        match self.deposited.get_mut(&data.tx) {
            Some(deposited) => {
                // check if the tx is under dispute. If not, ignore the resolve.
                if deposited.disputed {
                    self.available = self.available + deposited.amount;
                    self.held = self.held - deposited.amount;
                    // Dispute is considered resolved. The "disputed" boolean field
                    // now updates to false.
                    deposited.disputed = false;
                } else {
                    info!(
                        "Transaction #{}: Client #{}. Transaction is not under dispute. Resolve is ignored.",
                        data.tx, data.client
                        );
                }
            },
            None => info!(
                "transaction #{}: Client #{}. Cannot find the deposit transaction related to this resolve. Either the tx specified by the resolve doesn't exist or the specified tx is not a deposit. Resolve is ignored.",
                data.tx, data.client),
        }
    }

    pub fn chargeback(&mut self, data: &Transaction) {
        match self.deposited.get_mut(&data.tx) {
            Some(deposited) => {
                // check if the tx is under dispute. If not, ignore the chargeback.
                if deposited.disputed {
                    self.held = self.held - deposited.amount;
                    self.total = self.total - deposited.amount;
                    // A chargeback is the final state of a dispute. The "disputed" boolean field
                    // now updates to false.
                    deposited.disputed = false;
                    // Once a chargeback occurs, the client's account should be immediately frozen.
                    self.locked = true;
                } else {
                    info!(
                        "Transaction #{}: Client #{}. Transaction is not under dispute. Chargeback is ignored.",
                        data.tx, data.client
                        );
                }
            },
            None => info!(
                "transaction #{}: Client #{}. Cannot find the deposit transaction related to this chargeback. Either the tx specified by the chargeback doesn't exist or the specified tx is not a deposit. Chargeback is ignored.",
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
            _ => info!(
                "transaction #{}: Client #{}. Transaction type is not specified. Transaction is ignored.",
                data.tx, data.client
                ),
        }
    }
}

pub fn process_records<R: io::Read>(rdr: R) -> Result<HashMap<u16, Account>, Error> {
    let mut reader = csv::Reader::from_reader(rdr);

    // trim whitespaces in headers
    let headers = reader.headers().unwrap();
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
    fn test_process_records() -> Result<(), Error> {
        let test_file_path = "test_data.csv";
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
                        disputed: false,
                    },
                ),
                (
                    3,
                    Deposit {
                        amount: dec!(2),
                        disputed: false,
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
                    disputed: false,
                },
            )]),
        };
        let client3 = Account {
            client: 3,
            available: dec!(0),
            held: dec!(0),
            total: dec!(0),
            locked: false,
            deposited: HashMap::new(),
        };

        assert_eq!(*test_accounts.get(&1).unwrap(), client1);
        assert_eq!(*test_accounts.get(&2).unwrap(), client2);
        assert_eq!(*test_accounts.get(&3).unwrap(), client3);
        Ok(())
    }
}
