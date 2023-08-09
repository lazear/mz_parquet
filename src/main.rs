use anyhow::anyhow;
use clap::{Args, Command, FromArgMatches};
use sage_cloudpath::CloudPath;
use tokio::task::block_in_place;

pub mod mzml;
pub mod reader;
pub mod writer;

#[derive(Args, Debug)]
struct ConverterArgs {
    #[arg(short, long)]
    output_directory: Option<String>,

    #[arg(num_args(1..))]
    files: Vec<String>,
}

async fn convert_mzml(path: &str, output_directory: Option<&str>) -> anyhow::Result<()> {
    let cloudpath = path.parse::<CloudPath>()?;
    let pqt_path = match output_directory {
        Some(dir) => {
            let mut dir = dir.parse::<CloudPath>()?;
            dir.mkdir()?;

            let filename = cloudpath
                .filename()
                .and_then(|f| f.split_once('.'))
                .and_then(|(f, _)| Some(format!("{}.mzparquet", f)))
                .ok_or_else(|| anyhow!("no filename!"))?;
            dir.push(filename);
            dir
        }
        None => {
            let filename = cloudpath
                .filename()
                .and_then(|f| f.split_once('.'))
                .and_then(|(f, _)| Some(format!("{}.mzparquet", f)))
                .ok_or_else(|| anyhow!("no filename!"))?;
            match cloudpath.clone() {
                CloudPath::S3 { bucket, .. } => CloudPath::S3 {
                    bucket,
                    key: filename,
                },
                CloudPath::Local(path) => CloudPath::Local(path.with_file_name(filename)),
            }
        }
    };

    let mzml_spectra = mzml::MzMLReader::default()
        .parse(cloudpath.read().await?)
        .await?;

    let buffer = block_in_place(|| writer::serialize_to_parquet(Vec::new(), &mzml_spectra))?;

    let bytes = bytes::Bytes::from(buffer);
    let mzparquet_spectra = reader::deserialize_from_parquet(bytes.clone())?;

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

    pqt_path.write_bytes(bytes.into()).await?;

    log::info!(
        "copied {} spectra from {} to {}, and validated that data can be read back in a lossless manner",
        mzml_spectra.len(),
        cloudpath,
        pqt_path,
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::default()
        .filter_level(log::LevelFilter::Error)
        .parse_env(env_logger::Env::default().filter_or("LOG", "error,mz_parquet=info"))
        .init();

    let cli = Command::new("mz_parquet")
        .version(clap::crate_version!())
        .author("Michael Lazear <michaellazear92@gmail.com>")
        .about("Convert mzML to mzparquet");

    let matches = ConverterArgs::augment_args(cli).get_matches();
    let args = ConverterArgs::from_arg_matches(&matches)?;

    for file in args.files {
        convert_mzml(&file, args.output_directory.as_deref()).await?;
    }
    Ok(())
}
