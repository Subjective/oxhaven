//! From the bytes of a file to the text of each cell, column by column.
//!
//! The first half is the cell reader: one cell at a time, with what ended it. The second
//! is the row logic: which row holds the headers, which rows are skipped, and what a row
//! with too few or too many cells does.

use crate::{CsvError, ReadOptions};

fn is_space_or_tab(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

/// `bytes` without the spaces and tabs at either end.
///
/// Behavior: `io.deephaven.csv.reading.ReaderUtil#trimSpacesAndTabs`
pub(crate) fn trim_spaces_and_tabs(mut bytes: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = bytes {
        if !is_space_or_tab(*first) {
            break;
        }
        bytes = rest;
    }
    while let [rest @ .., last] = bytes {
        if !is_space_or_tab(*last) {
            break;
        }
        bytes = rest;
    }
    bytes
}

/// One cell and what followed it.
struct Cell {
    text: Vec<u8>,
    last_in_row: bool,
    end_of_input: bool,
}

/// Reads the cells of a delimited file, one after the other.
///
/// Behavior: `io.deephaven.csv.reading.cells.DelimitedCellGrabber`, without an escape
/// character, which `deephaven.csv.read` cannot set
struct Grabber<'a> {
    input: &'a [u8],
    offset: usize,
    quote: u8,
    delimiter: u8,
    ignore_surrounding_spaces: bool,
    trim: bool,
    /// The number of line ends passed, for the row number of an error.
    physical_row: usize,
}

impl Grabber<'_> {
    /// Behavior: `DelimitedCellGrabber#grabNext`
    fn grab_next(&mut self) -> Result<Cell, CsvError> {
        if self.ignore_surrounding_spaces {
            // a delimiter that is a space or a tab is not skipped
            while let Some(&byte) = self.input.get(self.offset) {
                if byte == self.delimiter || !is_space_or_tab(byte) {
                    break;
                }
                self.offset += 1;
            }
        }
        if self.input.get(self.offset) == Some(&self.quote) {
            self.offset += 1;
            let mut cell = self.quoted()?;
            if self.trim {
                cell.text = trim_spaces_and_tabs(&cell.text).to_vec();
            }
            Ok(cell)
        } else {
            let start = self.offset;
            let (end, last_in_row, end_of_input) = self.finish_field();
            let mut text = &self.input[start..end];
            if self.ignore_surrounding_spaces {
                text = trim_spaces_and_tabs(text);
            }
            Ok(Cell {
                text: text.to_vec(),
                last_in_row,
                end_of_input,
            })
        }
    }

    /// The rest of a cell whose opening quote was just read: up to the closing quote, a
    /// doubled quote being one quote of the text, and then nothing but spaces and tabs up
    /// to the end of the cell.
    ///
    /// Behavior: `DelimitedCellGrabber#processQuotedMode`
    fn quoted(&mut self) -> Result<Cell, CsvError> {
        let mut text = Vec::new();
        let mut start = self.offset;
        let mut after_carriage_return = false;
        loop {
            let Some(&byte) = self.input.get(self.offset) else {
                let message = "Cell did not have closing quote character";
                return Err(CsvError::Invalid(message.to_string()));
            };
            self.offset += 1;
            if byte == b'\r' {
                self.physical_row += 1;
                after_carriage_return = true;
                continue;
            }
            if byte == b'\n' {
                if !after_carriage_return {
                    self.physical_row += 1;
                }
                after_carriage_return = false;
                continue;
            }
            after_carriage_return = false;
            if byte != self.quote {
                continue;
            }
            if self.input.get(self.offset) != Some(&self.quote) {
                break;
            }
            // a doubled quote: keep the first and skip the second
            text.extend_from_slice(&self.input[start..self.offset]);
            self.offset += 1;
            start = self.offset;
        }
        text.extend_from_slice(&self.input[start..self.offset - 1]);
        let rest = self.offset;
        let (end, last_in_row, end_of_input) = self.finish_field();
        if !self.input[rest..end].iter().copied().all(is_space_or_tab) {
            // the text of the RuntimeException Java throws here
            let message = "Logic error: final non-whitespace in field is not quoteChar";
            return Err(CsvError::Invalid(message.to_string()));
        }
        Ok(Cell {
            text,
            last_in_row,
            end_of_input,
        })
    }

    /// Moves past the end of the cell: a delimiter, a line end (`\n`, `\r\n` or `\r`) or
    /// the end of the input. Gives where the cell's text ends, whether it is the last of
    /// its row, and whether the input ended.
    ///
    /// Behavior: `DelimitedCellGrabber#finishField`
    fn finish_field(&mut self) -> (usize, bool, bool) {
        loop {
            let Some(&byte) = self.input.get(self.offset) else {
                return (self.offset, true, true);
            };
            let end = self.offset;
            self.offset += 1;
            if byte == self.delimiter {
                return (end, false, false);
            }
            if byte == b'\n' {
                self.physical_row += 1;
                return (end, true, false);
            }
            if byte == b'\r' {
                if self.input.get(self.offset) == Some(&b'\n') {
                    self.offset += 1;
                }
                self.physical_row += 1;
                return (end, true, false);
            }
        }
    }

    /// The cells of the next row, or `None` at the end of the input.
    ///
    /// Behavior: `io.deephaven.csv.reading.headers.DelimitedHeaderFinder#tryReadOneRow`
    fn try_read_one_row(&mut self) -> Result<Option<Vec<Vec<u8>>>, CsvError> {
        let mut cells = Vec::new();
        loop {
            let cell = self.grab_next()?;
            cells.push(cell.text);
            if cell.last_in_row {
                if cells.len() == 1 && cells[0].is_empty() && cell.end_of_input {
                    return Ok(None);
                }
                return Ok(Some(cells));
            }
        }
    }
}

/// The cells of one column, in row order.
#[derive(Default)]
pub(crate) struct CellColumn {
    bytes: Vec<u8>,
    ends: Vec<usize>,
}

impl CellColumn {
    fn push(&mut self, cell: &[u8]) {
        self.bytes.extend_from_slice(cell);
        self.ends.push(self.bytes.len());
    }

    pub(crate) fn len(&self) -> usize {
        self.ends.len()
    }

    pub(crate) fn get(&self, row: usize) -> &[u8] {
        let start = if row == 0 { 0 } else { self.ends[row - 1] };
        &self.bytes[start..self.ends[row]]
    }

    #[cfg(test)]
    pub(crate) fn of(cells: &[&str]) -> Self {
        let mut column = CellColumn::default();
        for cell in cells {
            column.push(cell.as_bytes());
        }
        column
    }
}

/// What a file holds before any cell is given a type.
pub(crate) struct Cells {
    /// The column names as the file or the caller gives them, not yet made legal.
    pub(crate) headers: Vec<String>,
    pub(crate) columns: Vec<CellColumn>,
    pub(crate) rows: usize,
}

enum RowResult {
    EndOfInput,
    IgnoredEmptyRow,
    Processed,
}

/// The data rows: each cell goes to its column, or nowhere while rows are skipped.
///
/// Behavior: `io.deephaven.csv.reading.ParseInputToDenseStorage.RowAppender`
struct RowAppender<'a, 'b> {
    grabber: Grabber<'a>,
    options: &'b ReadOptions,
    headers: &'b [String],
    first_data_row: Option<Vec<Vec<u8>>>,
    /// The number of cells a row has. One more than `columns` when the last header is
    /// empty: the last cell of each row is then required to be empty, and dropped.
    width: usize,
    columns: Vec<CellColumn>,
}

impl RowAppender<'_, '_> {
    fn append(&mut self, column: usize, cell: &[u8], keep: bool) -> Result<(), CsvError> {
        if !keep {
            return Ok(());
        }
        match self.columns.get_mut(column) {
            Some(cells) => cells.push(cell),
            None if cell.is_empty() => {}
            None => {
                let message = "Column assumed empty but contains data";
                return Err(CsvError::Invalid(message.to_string()));
            }
        }
        Ok(())
    }

    /// Behavior: `ParseInputToDenseStorage#describeColumnHeader`
    fn describe(&self, column: usize) -> String {
        match self.headers.get(column) {
            Some(header) => format!("\"{header}\""),
            None => format!("(Column {column})"),
        }
    }

    /// Behavior: `ParseInputToDenseStorage.RowAppender#processNextRow`
    fn process_next_row(&mut self, keep: bool) -> Result<RowResult, CsvError> {
        if let Some(row) = self.first_data_row.take() {
            for (column, cell) in row.iter().enumerate() {
                self.append(column, cell, keep)?;
            }
            return Ok(RowResult::Processed);
        }
        let row_number = self.grabber.physical_row + 1;
        let mut column = 0;
        let mut last_in_row = false;
        while column < self.width {
            let located = |error: CsvError, column: String| match error {
                CsvError::Invalid(message) => CsvError::Invalid(format!(
                    "While processing row {row_number}, column {column}: {message}"
                )),
                other => other,
            };
            let cell = match self.grabber.grab_next() {
                Ok(cell) => cell,
                Err(error) => return Err(located(error, self.describe(column))),
            };
            last_in_row = cell.last_in_row;
            if last_in_row && column == 0 && cell.text.is_empty() {
                if cell.end_of_input {
                    return Ok(RowResult::EndOfInput);
                }
                if self.options.ignore_empty_lines {
                    return Ok(RowResult::IgnoredEmptyRow);
                }
            }
            if let Err(error) = self.append(column, &cell.text, keep) {
                return Err(located(error, self.describe(column)));
            }
            column += 1;
            if last_in_row {
                break;
            }
        }
        if !last_in_row {
            if !self.options.ignore_excess_columns {
                return Err(CsvError::Invalid(format!(
                    "Row {row_number} has too many columns (expected {})",
                    self.width
                )));
            }
            while !self.grabber.grab_next()?.last_in_row {}
        }
        if column >= self.width {
            return Ok(RowResult::Processed);
        }
        if !self.options.allow_missing_columns {
            return Err(CsvError::Invalid(format!(
                "Row {row_number} has too few columns (expected {})",
                self.width
            )));
        }
        // the cells that are missing are null: the empty text
        while column < self.width {
            self.append(column, b"", keep)?;
            column += 1;
        }
        Ok(RowResult::Processed)
    }
}

/// Reads the headers and the cells of `input`.
///
/// Behavior: `io.deephaven.csv.reading.headers.DelimitedHeaderFinder#determineHeadersToUse`,
/// `io.deephaven.csv.reading.CsvReader#delimitedReadLogic` and
/// `io.deephaven.csv.reading.ParseInputToDenseStorage#doit`
pub(crate) fn read_cells(input: &[u8], options: &ReadOptions) -> Result<Cells, CsvError> {
    let mut grabber = Grabber {
        input,
        offset: 0,
        // both are ASCII: `ReadOptions::check` has passed
        quote: options.quote as u8,
        delimiter: options.delimiter as u8,
        ignore_surrounding_spaces: options.ignore_surrounding_spaces,
        trim: options.trim,
        physical_row: 0,
    };
    let mut headers: Option<Vec<String>> = None;
    if options.has_header_row {
        let mut skip = options.skip_header_rows;
        let row = loop {
            let Some(row) = grabber.try_read_one_row()? else {
                let message = "Can't proceed because hasHeaderRow is set but input file is \
                               empty or shorter than skipHeaderRows";
                return Err(CsvError::Invalid(message.to_string()));
            };
            if skip == 0 {
                break row;
            }
            skip -= 1;
        };
        let text = |cell: Vec<u8>| String::from_utf8_lossy(&cell).into_owned();
        headers = Some(row.into_iter().map(text).collect());
    }
    if !options.columns.is_empty() {
        headers = Some(options.columns.iter().map(|c| c.name.clone()).collect());
    }
    let mut first_data_row = None;
    let mut headers = match headers {
        Some(headers) => headers,
        None => {
            let Some(row) = grabber.try_read_one_row()? else {
                let message = "Can't proceed because input file is empty and client has not \
                               specified headers";
                return Err(CsvError::Invalid(message.to_string()));
            };
            // Behavior: `io.deephaven.csv.reading.ReaderUtil#makeSyntheticHeaders`
            let names = (1..=row.len()).map(|n| format!("Column{n}")).collect();
            first_data_row = Some(row);
            names
        }
    };
    let width = headers.len();
    // A last header that is empty is a file whose rows end in a delimiter.
    if headers.last().is_some_and(String::is_empty) {
        headers.pop();
    }
    let mut appender = RowAppender {
        grabber,
        options,
        headers: &headers,
        first_data_row,
        width,
        columns: headers.iter().map(|_| CellColumn::default()).collect(),
    };
    let mut skip = options.skip_rows;
    while skip != 0 {
        if let RowResult::EndOfInput = appender.process_next_row(false)? {
            break;
        }
        skip -= 1;
    }
    let mut rows = 0;
    // a line that is ignored for being empty still counts against the limit
    let mut remaining = options.num_rows;
    while remaining != 0 {
        match appender.process_next_row(true)? {
            RowResult::EndOfInput => break,
            RowResult::Processed => rows += 1,
            RowResult::IgnoredEmptyRow => {}
        }
        remaining -= 1;
    }
    let columns = appender.columns;
    Ok(Cells {
        headers,
        columns,
        rows,
    })
}
