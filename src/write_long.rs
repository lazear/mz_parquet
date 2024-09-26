use crate::mzml::RawSpectrum;
use parquet::{
    basic::ZstdLevel,
    column::reader::get_typed_column_reader,
    data_type::{FloatType, Int32Type},
    file::{
        properties::WriterProperties,
        reader::FileReader,
        writer::{
            SerializedFileWriter, SerializedPageWriter, SerializedRowGroupWriter, TrackedWrite,
        },
    },
    schema::types::{ColumnDescriptor, SchemaDescriptor, Type},
};
use std::{collections::HashMap, io::Write, sync::Arc};

pub fn build_schema() -> parquet::errors::Result<Type> {
    let msg = r#"
        message schema {
            required int32 scan;
            required int32 level;
            required float rt;
            required float mz; 
            required float intensity;
            optional float ion_mobility;
            optional float isolation_lower;
            optional float isolation_upper;
            optional int32 precursor_scan;
            optional float precursor_mz;
            optional int32 precursor_charge;
        }
    "#;
    let schema = parquet::schema::parser::parse_message_type(msg)?;

    Ok(schema)
}

pub struct ColumnWriter<T: parquet::data_type::DataType, const NULLABLE: bool = false> {
    values: Vec<T::T>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
    column: Arc<ColumnDescriptor>,
    options: Arc<WriterProperties>,
}

impl<T: parquet::data_type::DataType, const NULLABLE: bool> ColumnWriter<T, NULLABLE> {
    pub fn new(column: Arc<ColumnDescriptor>, options: Arc<WriterProperties>) -> Self {
        Self {
            values: Default::default(),
            def_levels: Default::default(),
            rep_levels: Default::default(),
            column,
            options,
        }
    }

    fn write_and_flush<W: std::io::Write + Send>(
        &mut self,
        rg: &mut SerializedRowGroupWriter<'_, W>,
    ) -> anyhow::Result<()> {
        let mut buf = TrackedWrite::new(Vec::new());
        let page_writer = Box::new(SerializedPageWriter::new(&mut buf));
        let mut column = parquet::column::writer::ColumnWriterImpl::<T>::new(
            self.column.clone(),
            self.options.clone(),
            page_writer,
        );

        if NULLABLE {
            column.write_batch(&self.values, Some(&self.def_levels), None)?
        } else {
            column.write_batch(&self.values, None, None)?
        };

        let c = column.close().unwrap();
        buf.flush()?;
        let r = buf.into_inner()?;
        rg.append_column(&bytes::Bytes::from(r), c)?;

        self.values.clear();
        self.def_levels.clear();

        Ok(())
    }
}

impl<T: parquet::data_type::DataType> ColumnWriter<T, false> {
    pub fn extend<I: Iterator<Item = T::T>>(&mut self, iter: I) {
        self.values.extend(iter)
    }
}

impl<T: parquet::data_type::DataType> ColumnWriter<T, true> {
    pub fn extend<I: Iterator<Item = Option<T::T>>>(&mut self, iter: I) {
        for item in iter {
            if let Some(inner) = item {
                self.values.push(inner);
                self.def_levels.push(1);
            } else {
                self.def_levels.push(0);
            }
            // self.rep_levels.push(0);
        }
    }
}

pub struct ChunkWriter<'a, W>
where
    W: std::io::Write + Send,
{
    writer: &'a mut SerializedFileWriter<W>,
    current_rows: usize,
    scans_written: usize,
    spectrum_ref_to_scan: HashMap<Vec<u8>, u32>,

    scan: ColumnWriter<Int32Type>,
    level: ColumnWriter<Int32Type>,
    rt: ColumnWriter<FloatType>,
    mz: ColumnWriter<FloatType>,
    int: ColumnWriter<FloatType>,
    ionmobility: ColumnWriter<FloatType, true>,
    lo: ColumnWriter<FloatType, true>,
    hi: ColumnWriter<FloatType, true>,
    pscan: ColumnWriter<Int32Type, true>,
    pmz: ColumnWriter<FloatType, true>,
    pz: ColumnWriter<Int32Type, true>,
}

impl<'a, W> ChunkWriter<'a, W>
where
    W: std::io::Write + Send,
{
    pub fn new(
        writer: &'a mut SerializedFileWriter<W>,
        descr: &SchemaDescriptor,
        options: Arc<WriterProperties>,
    ) -> Self {
        assert_eq!(descr.num_columns(), 11);

        Self {
            current_rows: 0,
            scans_written: 0,
            writer,
            spectrum_ref_to_scan: Default::default(),
            scan: ColumnWriter::new(descr.column(0), options.clone()),
            level: ColumnWriter::new(descr.column(1), options.clone()),
            rt: ColumnWriter::new(descr.column(2), options.clone()),
            mz: ColumnWriter::new(descr.column(3), options.clone()),
            int: ColumnWriter::new(descr.column(4), options.clone()),
            ionmobility: ColumnWriter::new(descr.column(5), options.clone()),
            lo: ColumnWriter::new(descr.column(6), options.clone()),
            hi: ColumnWriter::new(descr.column(7), options.clone()),
            pscan: ColumnWriter::new(descr.column(8), options.clone()),
            pmz: ColumnWriter::new(descr.column(9), options.clone()),
            pz: ColumnWriter::new(descr.column(10), options.clone()),
        }
    }

    /// Write a spectrum to an mzparquet file. This function may have IO operations,
    /// if writing this spectrum would fill up the current row group.
    pub fn write_spectrum(&mut self, spectrum: &RawSpectrum) -> anyhow::Result<()> {
        let n = spectrum.mz.len();
        self.spectrum_ref_to_scan
            .insert(spectrum.id.clone(), self.scans_written as u32);

        self.scan
            .extend(std::iter::repeat(self.scans_written as i32).take(n));
        self.level
            .extend(std::iter::repeat(spectrum.ms_level as i32).take(n));
        self.rt
            .extend(std::iter::repeat(spectrum.scan_start_time).take(n));
        self.mz.extend(spectrum.mz.iter().copied());
        self.int.extend(spectrum.intensity.iter().copied());
        self.ionmobility
            .extend(std::iter::repeat(spectrum.inverse_ion_mobility).take(n));

        if let Some(precursor) = spectrum.precursors.get(0) {
            let precursor_scan = precursor
                .spectrum_ref
                .as_ref()
                .map(|s| self.spectrum_ref_to_scan.get(s))
                .flatten();

            let lo = precursor.isolation_window_lower.map(|w| precursor.mz - w);
            let hi = precursor.isolation_window_upper.map(|w| precursor.mz + w);

            self.lo.extend(std::iter::repeat(lo).take(n));
            self.hi.extend(std::iter::repeat(hi).take(n));

            self.pmz
                .extend(std::iter::repeat(Some(precursor.mz)).take(n));
            self.pz
                .extend(std::iter::repeat(precursor.charge.map(|z| z as i32)).take(n));
            self.pscan
                .extend(std::iter::repeat(precursor_scan.map(|z| *z as i32)).take(n));
        } else {
            self.lo.extend(std::iter::repeat(None).take(n));
            self.hi.extend(std::iter::repeat(None).take(n));
            self.pmz.extend(std::iter::repeat(None).take(n));
            self.pz.extend(std::iter::repeat(None).take(n));
            self.pscan.extend(std::iter::repeat(None).take(n));
        }

        self.scans_written += 1;
        self.current_rows += n;

        // If we have more than 2^18 ions in this row group, write it to buffer
        // and reset all of the columns
        if n >= 2usize.pow(18) {
            self.write_to_row_group()?;
        }

        Ok(())
    }

    #[must_use]
    pub fn finish(mut self) -> anyhow::Result<()> {
        if self.current_rows > 0 {
            self.write_to_row_group()?;
        }
        Ok(())
    }

    fn write_to_row_group(&mut self) -> anyhow::Result<()> {
        let mut rg = self.writer.next_row_group()?;

        self.scan.write_and_flush(&mut rg)?;
        self.level.write_and_flush(&mut rg)?;
        self.rt.write_and_flush(&mut rg)?;
        self.mz.write_and_flush(&mut rg)?;
        self.int.write_and_flush(&mut rg)?;
        self.ionmobility.write_and_flush(&mut rg)?;
        self.lo.write_and_flush(&mut rg)?;
        self.hi.write_and_flush(&mut rg)?;
        self.pscan.write_and_flush(&mut rg)?;
        self.pmz.write_and_flush(&mut rg)?;
        self.pz.write_and_flush(&mut rg)?;

        rg.close()?;

        // We have written and cleared all buffers, reset number of written rows
        self.current_rows = 0;

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

    let mut chunk_writer = ChunkWriter::new(&mut writer, &sd, options);

    for spectrum in spectra {
        chunk_writer.write_spectrum(spectrum)?;
    }
    chunk_writer.finish()?;
    Ok(writer.into_inner()?)
}
