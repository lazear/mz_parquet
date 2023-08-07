use std::{
    io::{BufWriter, Write},
    path::Path,
};

use mzml::RawSpectrum;
use parquet::{
    basic::ZstdLevel,
    data_type::{BoolType, ByteArrayType, FloatType, Int32Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use tokio::{io::BufReader, task::block_in_place};

use crate::mzml::Precursor;

pub mod mzml;
pub fn build_schema() -> parquet::errors::Result<Type> {
    let msg = r#"
        message schema {
            required byte_array id (utf8);
            required int32 ms_level;
            required boolean centroid;
            required float scan_start_time;
            optional float inverse_ion_mobility;
            required float ion_injection_time;
            required float total_ion_current;
            optional group precursors (list) {
                repeated group list {
                    required group element {
                        required float selected_ion_mz;
                        optional int32 selected_ion_charge;
                        optional float selected_ion_intensity;
                        optional float isolation_window_target;
                        optional float isolation_window_lower;
                        optional float isolation_window_upper;
                        optional byte_array spectrum_ref (utf8);
                    }
                }
            }
            required group mz (LIST) {
                repeated group list {
                    required float element;
                }
            }
            required group intensity (LIST) {
                repeated group list {
                    required float element;
                }
            }
            optional group cv_params (list) {
                repeated group list {
                    required group element {
                        required byte_array accession (utf8);
                        required byte_array value (utf8);
                    }
                }
            }

        }
    "#;
    let schema = parquet::schema::parser::parse_message_type(msg)?;
    Ok(schema)
}

fn serialize_to_parquet<W: Write + Send>(
    w: W,
    spectra: &[RawSpectrum],
) -> parquet::errors::Result<W> {
    let schema = build_schema()?;

    let options = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();

    let mut writer = SerializedFileWriter::new(w, schema.into(), options.into())?;
    for spectra in spectra.chunks(4096) {
        let mut rg = writer.next_row_group()?;

        macro_rules! write_col {
            ($lambda:expr, $ty:ident) => {
                if let Some(mut col) = rg.next_column()? {
                    col.typed::<$ty>().write_batch(
                        &spectra.iter().map($lambda).collect::<Vec<_>>(),
                        None,
                        None,
                    )?;
                    col.close()?;
                }
            };
        }

        macro_rules! write_precursor {
            ($lambda:expr, $ty:ident, $max_level:expr) => {
                if let Some(mut col) = rg.next_column()? {
                    let mut values = Vec::new();

                    let mut def_levels = Vec::new();
                    let mut rep_levels = Vec::new();

                    for spectrum in spectra {
                        if spectrum.precursors.is_empty() {
                            def_levels.push(0);
                            rep_levels.push(0);
                        } else {
                            for precursor in &spectrum.precursors {
                                match $lambda(precursor) {
                                    Some(value) => {
                                        values.push(value);
                                        def_levels.push($max_level)
                                    }
                                    None => def_levels.push($max_level - 1),
                                }
                            }
                            rep_levels.extend(
                                std::iter::once(0)
                                    .chain(std::iter::repeat(1))
                                    .take(spectrum.precursors.len()),
                            );
                        }
                    }

                    col.typed::<$ty>().write_batch(
                        &values,
                        Some(&def_levels),
                        Some(&rep_levels),
                    )?;
                    col.close()?;
                };
            };
        }

        write_col!(|s| s.id.as_slice().into(), ByteArrayType);
        write_col!(|s| s.ms_level as i32, Int32Type);
        write_col!(|s| s.centroid, BoolType);
        write_col!(|s| s.scan_start_time, FloatType);

        // ion mobility
        if let Some(mut col) = rg.next_column()? {
            let values = spectra
                .iter()
                .filter_map(|s| s.inverse_ion_mobility)
                .collect::<Vec<_>>();
            let def_levels = spectra
                .iter()
                .map(|s| {
                    if s.inverse_ion_mobility.is_some() {
                        1
                    } else {
                        0
                    }
                })
                .collect::<Vec<_>>();

            col.typed::<FloatType>()
                .write_batch(&values, Some(&def_levels), None)?;
            col.close()?;
        }

        write_col!(|s| s.ion_injection_time, FloatType);
        write_col!(|s| s.total_ion_current, FloatType);

        write_precursor!(|p: &Precursor| Some(p.mz), FloatType, 2);
        write_precursor!(
            |p: &Precursor| p.charge.map(|charge| charge as i32),
            Int32Type,
            3
        );
        write_precursor!(|p: &Precursor| p.intensity, FloatType, 3);
        write_precursor!(|p: &Precursor| p.isolation_window_target, FloatType, 3);
        write_precursor!(|p: &Precursor| p.isolation_window_lower, FloatType, 3);
        write_precursor!(|p: &Precursor| p.isolation_window_upper, FloatType, 3);

        if let Some(mut col) = rg.next_column()? {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();

            for spectrum in spectra {
                if spectrum.precursors.is_empty() {
                    def_levels.push(0);
                    rep_levels.push(0);
                } else {
                    for precursor in &spectrum.precursors {
                        match precursor.spectrum_ref.as_ref() {
                            Some(value) => {
                                values.push(value.as_slice().into());
                                def_levels.push(3)
                            }
                            None => def_levels.push(2),
                        }
                    }
                    rep_levels.extend(
                        std::iter::once(0)
                            .chain(std::iter::repeat(1))
                            .take(spectrum.precursors.len()),
                    );
                }
            }

            col.typed::<ByteArrayType>().write_batch(
                &values,
                Some(&def_levels),
                Some(&rep_levels),
            )?;
            col.close()?;
        }

        if let Some(mut col) = rg.next_column()? {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();

            for spectrum in spectra {
                values.extend(spectrum.mz.iter().copied());
                def_levels.extend(std::iter::repeat(1).take(spectrum.mz.len()));
                rep_levels.extend(
                    (std::iter::once(0).chain(std::iter::repeat(1))).take(spectrum.mz.len()),
                );
            }
            col.typed::<FloatType>()
                .write_batch(&values, Some(&def_levels), Some(&rep_levels))?;
            col.close()?;
        }

        if let Some(mut col) = rg.next_column()? {
            let mut values = Vec::new();
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();

            for spectrum in spectra {
                values.extend(spectrum.intensity.iter().copied());
                def_levels.extend(std::iter::repeat(1).take(spectrum.intensity.len()));
                rep_levels.extend(
                    (std::iter::once(0).chain(std::iter::repeat(1))).take(spectrum.intensity.len()),
                );
            }
            col.typed::<FloatType>()
                .write_batch(&values, Some(&def_levels), Some(&rep_levels))?;
            col.close()?;
        }

        if let Some(mut col) = rg.next_column()? {
            let key = b"MS:1000133";
            let values = vec![key.as_slice().into(); spectra.len()];
            let def_levels = vec![2; spectra.len()];
            let rep_levels = vec![0; spectra.len()];

            col.typed::<ByteArrayType>().write_batch(
                &values,
                Some(&def_levels),
                Some(&rep_levels),
            )?;
            col.close()?;
        }
        if let Some(mut col) = rg.next_column()? {
            let values = spectra
                .iter()
                .map(|s| s.id.as_slice().into())
                .collect::<Vec<_>>();
            let def_levels = vec![2; spectra.len()];
            let rep_levels = vec![0; spectra.len()];

            col.typed::<ByteArrayType>().write_batch(
                &values,
                Some(&def_levels),
                Some(&rep_levels),
            )?;
            col.close()?;
        }

        rg.close()?;
    }

    writer.into_inner()
}

async fn convert_mzml(path: &str) -> anyhow::Result<()> {
    let pqt_path = Path::new(path).with_extension("mzparquet");

    let file = tokio::fs::File::open(path).await?;
    let rdr = BufReader::new(file);
    let spectra = mzml::MzMLReader::default().parse(rdr).await?;
    block_in_place(|| {
        std::fs::File::create(&pqt_path)
            .map_err(Into::into)
            .and_then(|file| serialize_to_parquet(BufWriter::new(file), &spectra))
            .map_err(Into::into)
            .and_then(|mut wtr| wtr.flush())
    })?;
    log::info!(
        "copied {} spectra from {} to {}",
        spectra.len(),
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
