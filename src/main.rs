use anyhow::anyhow;
use clap::{Args, Command, FromArgMatches};
use sage_cloudpath::CloudPath;

pub mod mzml;
pub mod reader;
pub mod write_long;

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

    let mut buffer = Vec::new();
    write_long::serialize_to_parquet(&mut buffer, &mzml_spectra)?;

    pqt_path.write_bytes(buffer).await?;

    log::info!(
        "copied {} spectra from {} to {}",
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
        let output = args.output_directory.clone();
        convert_mzml(&file, output.as_deref()).await?
    }

    Ok(())
}
