use crate::mzml::{Precursor, RawSpectrum};
use parquet::{
    basic::ZstdLevel,
    column::{reader::get_typed_column_reader, writer::ColumnCloseResult},
    data_type::{BoolType, ByteArrayType, DataType, FloatType, Int32Type},
    file::{
        properties::WriterProperties,
        reader::FileReader,
        writer::{
            SerializedFileWriter, SerializedPageWriter, SerializedRowGroupWriter, TrackedWrite,
        },
    },
    schema::types::{ColumnDescriptor, ColumnPath, Type},
};
use std::{io::Write, mem::MaybeUninit, sync::Arc};

pub fn build_schema() -> parquet::errors::Result<Type> {
    let msg = r#"
        message schema {
            required int32 scan;
            required int32 level;
            required float rt;
            required float mz; 
            required float intensity;
        }
    "#;
    // optional float isolation_window_lower;
    // optional float isolation_window_uppper;
    // optional float inverse_ion_mobility;
    // optional float precursor_mz;
    // optional float precursor_charge;
    let schema = parquet::schema::parser::parse_message_type(msg)?;

    Ok(schema)
}

pub struct ColumnWriter<T: parquet::data_type::DataType> {
    values: Vec<T::T>,
    column: Arc<ColumnDescriptor>,
    options: Arc<WriterProperties>,
}

impl<T: parquet::data_type::DataType> ColumnWriter<T> {
    pub fn new(column: Arc<ColumnDescriptor>, options: Arc<WriterProperties>) -> Self {
        Self {
            values: Default::default(),
            column,
            options,
        }
    }

    pub fn extend<I: Iterator<Item = T::T>>(&mut self, iter: I) {
        self.values.extend(iter)
    }
}

pub trait TypeEraseColumnWriter<W: Write + Send> {
    fn write_column(
        &self,
        offset: usize,
        length: usize,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> anyhow::Result<()>;
}

impl<T: DataType, W: Write + Send> TypeEraseColumnWriter<W> for ColumnWriter<T> {
    fn write_column(
        &self,
        offset: usize,
        length: usize,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> anyhow::Result<()> {
        let mut buf = TrackedWrite::new(Vec::new());
        let page_writer = Box::new(SerializedPageWriter::new(&mut buf));
        let mut column = parquet::column::writer::ColumnWriterImpl::<T>::new(
            self.column.clone(),
            self.options.clone(),
            page_writer,
        );
        let x = column.write_batch(&self.values[offset..offset + length], None, None)?;

        let c = column.close().unwrap();
        buf.flush()?;
        let r = buf.into_inner()?;
        rg.append_column(&bytes::Bytes::from(r), c)?;

        Ok(())
    }
}

pub fn serialize_to_parquet<W: Write + Send>(w: W, spectra: &[RawSpectrum]) -> anyhow::Result<W> {
    let schema = build_schema()?;
    let sd = parquet::schema::types::SchemaDescriptor::new(schema.clone().into());

    let options = Arc::new(
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_dictionary_enabled(false)
            .build(),
    );

    let mut writer = SerializedFileWriter::new(w, schema.into(), options.clone())?;

    let pb = indicatif::ProgressBar::new(spectra.len() as u64)
        .with_message("Writing mzparquet")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .unwrap(),
        );

    let mut total_rows = 0;

    let mut c_scan: ColumnWriter<Int32Type> = ColumnWriter::new(sd.column(0), options.clone());
    let mut c_level: ColumnWriter<Int32Type> = ColumnWriter::new(sd.column(1), options.clone());
    let mut c_rt: ColumnWriter<FloatType> = ColumnWriter::new(sd.column(2), options.clone());
    let mut c_mz: ColumnWriter<FloatType> = ColumnWriter::new(sd.column(3), options.clone());
    let mut c_int: ColumnWriter<FloatType> = ColumnWriter::new(sd.column(4), options.clone());
    // let mut c_lo: ColumnWriter<FloatType> = ColumnWriter::new(sd.column(5), options.clone());
    // let mut c_hi: ColumnWriter<FloatType> = ColumnWriter::new(sd.column(6), options.clone());

    for (scan, spectra) in spectra.iter().enumerate() {
        let n = spectra.mz.len();
        c_scan.extend(std::iter::repeat(scan as i32).take(n));
        c_level.extend(std::iter::repeat(spectra.ms_level as i32).take(n));
        c_rt.extend(std::iter::repeat(spectra.scan_start_time).take(n));
        c_mz.extend(spectra.mz.iter().copied());
        c_int.extend(spectra.intensity.iter().copied());
        // let lo = spectra.precursors.get(0).and_then(|p| p.isolation_window_lower);
        // let hi = spectra.precursors.get(0).and_then(|p| p.isolation_window_upper);
        // c_lo.extend(std::iter::repeat(lo).take(n));
        // c_hi.extend(std::iter::repeat(hi).take(n));
        total_rows += n;
    }

    // pb.inc(spectra.len() as u64);

    let columns: [Box<dyn TypeEraseColumnWriter<W>>; 5] = [
        Box::new(c_scan),
        Box::new(c_level),
        Box::new(c_rt),
        Box::new(c_mz),
        Box::new(c_int),
    ];

    const RG_SIZE: usize = 2usize.pow(18);
    for n in (0..total_rows).step_by(RG_SIZE) {
        let length = (total_rows - n).min(RG_SIZE);
        let mut rg = writer.next_row_group()?;
        for col in &columns {
            col.write_column(n, length, &mut rg)?;
        }

        rg.close()?;
    }
    Ok(writer.into_inner()?)
}

pub fn deserialize_from_parquet<R: 'static + parquet::file::reader::ChunkReader>(
    r: R,
) -> parquet::errors::Result<()> {
    // let mut spectra = Vec::new();
    let reader = parquet::file::reader::SerializedFileReader::new(r)?;
    let nrows = reader.metadata().file_metadata().num_rows();

    let mut scans: Vec<i32> = vec![0; nrows as usize];

    let mut offset = 0;
    for row_group_idx in 0..reader.metadata().num_row_groups() {
        let rg = reader.get_row_group(row_group_idx)?;
        let col = rg.get_column_reader(0)?;
        let mut col = get_typed_column_reader::<Int32Type>(col);
        let nrows = rg.metadata().num_rows() as usize;
        // let mut values = vec![0; nrows] ;
        let (_, vals, _) = col.read_records(usize::MAX, None, None, &mut scans[offset..])?;
        assert_eq!(vals, nrows);
        offset += vals;
    }

    // for col in 0..rg.metadata().num_columns() {
    //     // dbg!(rg.metadata().column(col).column_descr());
    //     println!("{:#?}", rg.metadata().column(col));
    // }

    Ok(())
}
