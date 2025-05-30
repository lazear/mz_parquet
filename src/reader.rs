use crate::mzml::{Precursor, RawSpectrum};
use parquet::{
    errors::ParquetError,
    file::{
        reader::{ChunkReader, FileReader},
        serialized_reader::SerializedFileReader,
    },
    record::{Field, RowColumnIter},
};

trait ExtractFromField: Sized {
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

impl<T: ExtractFromField> ExtractFromField for Vec<T> {
    fn extract(field: &Field) -> parquet::errors::Result<Self> {
        match field {
            Field::ListInternal(list) => list.elements().iter().map(T::extract).collect(),
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

pub fn deserialize_from_parquet<R: 'static + ChunkReader>(
    r: R,
) -> parquet::errors::Result<Vec<RawSpectrum>> {
    let mut spectra = Vec::new();
    let reader = SerializedFileReader::new(r)?;
    let nrows = reader.metadata().file_metadata().num_rows();

    let pb = indicatif::ProgressBar::new(nrows as u64)
        .with_message("verifying mzparquet")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .unwrap(),
        );

    for row in reader.get_row_iter(None)? {
        let row = row?;
        let mut iter = row.get_column_iter();

        let spectrum = RawSpectrum {
            id: get_from_column_iter::<String>("id", &mut iter)?.into_bytes(),
            ms_level: get_from_column_iter("ms_level", &mut iter)?,
            centroid: get_from_column_iter("centroid", &mut iter)?,
            scan_start_time: get_from_column_iter("scan_start_time", &mut iter)?,
            collision_energy: get_from_column_iter("collision_energy", &mut iter)?,
            inverse_ion_mobility: get_from_column_iter("inverse_ion_mobility", &mut iter)?,
            ion_injection_time: get_from_column_iter("ion_injection_time", &mut iter)?,
            total_ion_current: get_from_column_iter("total_ion_current", &mut iter)?,
            precursors: get_from_column_iter::<Option<Vec<Precursor>>>("precursors", &mut iter)?
                .unwrap_or_default(),
            mz: get_from_column_iter("mz", &mut iter)?,
            intensity: get_from_column_iter("intensity", &mut iter)?,
            noise: Vec::new(),
        };
        spectra.push(spectrum);
        pb.inc(1);
    }

    Ok(spectra)
}
