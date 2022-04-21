use clap::{App, Arg};
use env_logger;
use payment_engine::process_records;
use std::error::Error;
use std::fs::File;
use std::io;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let matches = App::new("payment_engine")
        .version("1.0")
        .author("junjun Dong <junjun.dong9@gmail.com>")
        .about("a payments engine that reads transactions from CSV, updates client accounts, handles disputes and chargebacks, and then outputs the state of clients accounts as CSV")
        .arg(
            Arg::with_name("input-file-name")
                .help("Enter the input CSV file name")
                .required(true),
        )
        .get_matches();
    let path = matches.value_of("input-file-name").unwrap();
    let rdr = File::open(path)?;
    let mut writer = csv::Writer::from_writer(io::stdout());
    let accounts = process_records(rdr)?;
    for (_, val) in &accounts {
        writer.serialize(val)?;
    }
    writer.flush()?;
    Ok(())
}
