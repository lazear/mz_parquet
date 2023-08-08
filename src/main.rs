use std::{
    io::{BufWriter, Read, Write},
    path::Path,
};

use mzml::RawSpectrum;
use parquet::{
    basic::ZstdLevel,
    data_type::{BoolType, ByteArrayType, FloatType, Int32Type},
    errors::ParquetError,
    file::{
        properties::WriterProperties,
        reader::{ChunkReader, FileReader},
        serialized_reader::SerializedFileReader,
        writer::SerializedFileWriter,
    },
    record::{Field, RowAccessor, RowColumnIter},
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

pub trait ExtractFromField: Sized {
    fn extract(field: &Field) -> parquet::errors::Result<Self>;
}

impl ExtractFromField for String {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::Str(s) => Ok(s.to_owned()),
            _ => Err(ParquetError::General(
                "failed to extract field as a `string`".into(),
            )),
        }
    }
}

impl ExtractFromField for f32 {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::Float(f) => Ok(*f),
            _ => Err(ParquetError::General(
                "failed to extract field as a `f32`".into(),
            )),
        }
    }
}

impl ExtractFromField for u8 {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::Int(f) => Ok(*f as u8),
            _ => Err(ParquetError::General(
                "failed to extract field as a `u8`".into(),
            )),
        }
    }
}

impl ExtractFromField for bool {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::Bool(f) => Ok(*f),
            _ => Err(ParquetError::General(
                "failed to extract field as a `u8`".into(),
            )),
        }
    }
}

impl ExtractFromField for Precursor {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::Group(row) => {
                let mut iter = row.get_column_iter();
                let mz = get_from_column_iter("selected_ion_mz", &mut iter)?;
                let charge = get_from_column_iter("selected_ion_charge", &mut iter)?;
                let intensity = get_from_column_iter("selected_ion_intensity", &mut iter)?;
                let isolation_window_target =
                    get_from_column_iter("isolation_window_target", &mut iter)?;
                let isolation_window_lower =
                    get_from_column_iter("isolation_window_lower", &mut iter)?;
                let isolation_window_upper =
                    get_from_column_iter("isolation_window_upper", &mut iter)?;
                let spectrum_ref =
                    get_from_column_iter::<Option<String>>("spectrum_ref", &mut iter)?
                        .map(String::into_bytes);

                Ok(Precursor {
                    mz,
                    intensity,
                    charge,
                    isolation_window_target,
                    isolation_window_lower,
                    isolation_window_upper,
                    spectrum_ref,
                })
            }
            _ => Err(ParquetError::General(
                "failed to extract field as a `precursor`".into(),
            )),
        }
    }
}

impl<T: ExtractFromField> ExtractFromField for Vec<T> {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::ListInternal(list) => list.elements().into_iter().map(T::extract).collect(),
            _ => Err(ParquetError::General(
                "failed to extract field as a `list`".into(),
            )),
        }
    }
}

impl<T: ExtractFromField> ExtractFromField for Option<T> {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::Null => Ok(None),
            other => T::extract(other).map(Some),
        }
    }
}

fn get_from_column_iter<T: ExtractFromField>(
    name: &'static str,
    iter: &mut RowColumnIter<'_>,
) -> parquet::errors::Result<T> {
    let (header, field) = iter.next().ok_or_else(|| {
        ParquetError::General(format!(
            "could not extract field {} from row: unexpected end of columns!",
            name
        ))
    })?;
    if header == name {
        T::extract(field)
    } else {
        Err(ParquetError::General(format!(
            "tried to extract field {}, but got {} instead",
            name, header
        )))
    }
}

fn deserialize_from_parquet<R: 'static + ChunkReader>(
    r: R,
) -> parquet::errors::Result<Vec<RawSpectrum>> {
    let mut spectra = Vec::new();
    let reader = SerializedFileReader::new(r)?;
    for row in reader.get_row_iter(None)? {
        let row = row?;
        let mut iter = row.get_column_iter();

        let spectrum = RawSpectrum {
            id: get_from_column_iter::<String>("id", &mut iter)?.into_bytes(),
            ms_level: get_from_column_iter("ms_level", &mut iter)?,
            centroid: get_from_column_iter("centroid", &mut iter)?,
            scan_start_time: get_from_column_iter("scan_start_time", &mut iter)?,
            inverse_ion_mobility: get_from_column_iter("inverse_ion_mobility", &mut iter)?,
            ion_injection_time: get_from_column_iter("ion_injection_time", &mut iter)?,
            total_ion_current: get_from_column_iter("total_ion_current", &mut iter)?,
            precursors: get_from_column_iter::<Option<Vec<Precursor>>>("precursors", &mut iter)?
                .unwrap_or_default(),
            mz: get_from_column_iter("mz", &mut iter)?,
            intensity: get_from_column_iter("intensity", &mut iter)?,
            noise: Vec::new(),
        };
        // dbg!(&spectrum);
        spectra.push(spectrum);
        // break;
    }

    Ok(spectra)
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

    let a = std::time::Instant::now();
    let file = tokio::fs::File::open(path).await?;
    let rdr = BufReader::new(file);
    let spectra = mzml::MzMLReader::default().parse(rdr).await?;
    let b = std::time::Instant::now();
    block_in_place(|| {
        std::fs::File::create(&pqt_path)
            .map_err(Into::into)
            .and_then(|file| serialize_to_parquet(BufWriter::new(file), &spectra))
            .map_err(Into::into)
            .and_then(|mut wtr| wtr.flush())
    })?;

    let c = std::time::Instant::now();
    deserialize_from_parquet(std::fs::File::open(&pqt_path)?)?;
    let d = std::time::Instant::now();
    log::info!(
        "copied {} spectra from {} to {}",
        spectra.len(),
        path,
        pqt_path.display()
    );
    log::info!("reading mzML: {} ms", (b - a).as_millis());
    log::info!("reading mzparquet: {} ms", (d - c).as_millis());
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
