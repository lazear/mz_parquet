use crate::mzml::{Precursor, RawSpectrum};
use anyhow::Context;
use parquet::{
    basic::ZstdLevel,
    data_type::{BoolType, ByteArrayType, FloatType, Int32Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::{ColumnPath, Type},
};
use std::io::Write;

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
        }
    "#;
    let schema = parquet::schema::parser::parse_message_type(msg)?;
    Ok(schema)
}

pub fn serialize_to_parquet<W: Write + Send>(w: W, spectra: &[RawSpectrum]) -> anyhow::Result<W> {
    let schema = build_schema()?;

    let options = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_dictionary_enabled(false)
        .set_column_encoding(
            ColumnPath::new(vec!["mz".into()]),
            parquet::basic::Encoding::BYTE_STREAM_SPLIT,
        )
        .set_column_encoding(
            ColumnPath::new(vec!["intensity".into()]),
            parquet::basic::Encoding::BYTE_STREAM_SPLIT,
        )
        .build();

    let mut writer = SerializedFileWriter::new(w, schema.into(), options.into())?;

    let pb = indicatif::ProgressBar::new(spectra.len() as u64)
        .with_message("Writing mzparquet")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .unwrap(),
        );

    for spectra in spectra.chunks(u16::MAX as usize) {
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

        // write_precursor!(
        //     |p: &Precursor| p.spectrum_ref.as_ref().map(|s| s.as_slice().into()),
        //     ByteArrayType,
        //     3
        // );

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
                if spectrum.mz.is_empty() {
                    eprintln!("{:?}", spectrum);
                }
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

        // if let Some(mut col) = rg.next_column()? {
        //     // let key = b"MS:1000133";
        //     // let values = vec![key.as_slice().into(); spectra.len()];
        //     let def_levels = vec![0; spectra.len()];
        //     let rep_levels = vec![0; spectra.len()];

        //     col.typed::<ByteArrayType>().write_batch(
        //         &[],
        //         Some(&def_levels),
        //         Some(&rep_levels),
        //     )?;
        //     col.close()?;
        // }

        // if let Some(mut col) = rg.next_column()? {
        //     // let values = spectra
        //     //     .iter()
        //     //     .map(|s| s.id.as_slice().into())
        //     //     .collect::<Vec<_>>();
        //     let def_levels = vec![0; spectra.len()];
        //     let rep_levels = vec![0; spectra.len()];

        //     col.typed::<ByteArrayType>().write_batch(
        //         &[],
        //         Some(&def_levels),
        //         Some(&rep_levels),
        //     )?;
        //     col.close()?;
        // }

        rg.close()?;
        pb.inc(spectra.len() as u64);
    }

    writer.into_inner().context("failed")
}
