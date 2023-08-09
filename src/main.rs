use std::{
    io::{BufWriter, Write},
    path::Path,
};
use tokio::{io::BufReader, task::block_in_place};

pub mod mzml;
pub mod reader;
pub mod writer;

async fn convert_mzml(path: &str) -> anyhow::Result<()> {
    let pqt_path = Path::new(path).with_extension("mzparquet");

    let file = tokio::fs::File::open(path).await?;
    let rdr = BufReader::new(file);
    let mzml_spectra = mzml::MzMLReader::default().parse(rdr).await?;

    block_in_place(|| {
        std::fs::File::create(&pqt_path)
            .map_err(Into::into)
            .and_then(|file| writer::serialize_to_parquet(BufWriter::new(file), &mzml_spectra))
            .map_err(Into::into)
            .and_then(|mut wtr| wtr.flush())
    })?;

    let mzparquet_spectra = reader::deserialize_from_parquet(std::fs::File::open(&pqt_path)?)?;

    assert_eq!(
        mzml_spectra.len(),
        mzparquet_spectra.len(),
        "Read {} spectra from mzML, and {} from converted mzparquet file!",
        mzml_spectra.len(),
        mzparquet_spectra.len()
    );

    for (a, b) in mzml_spectra.iter().zip(mzparquet_spectra.iter()) {
        assert_eq!(a, b);
    }

    log::info!(
        "copied {} spectra from {} to {}, and validated that mzML data == mzparquet data",
        mzml_spectra.len(),
        path,
        pqt_path.display()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::default()
        .filter_level(log::LevelFilter::Error)
        .parse_env(env_logger::Env::default().filter_or("LOG", "error,mz_parquet=info"))
        .init();

    let args = std::env::args().skip(1);
    for arg in args {
        convert_mzml(&arg).await?;
    }
    Ok(())
}
