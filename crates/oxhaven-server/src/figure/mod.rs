//! Figures: what a client is sent to draw a plot.
//!
//! A [`FigureWidget`] is a finished description of a figure, the `FigureDescriptor` of the
//! protocol, with the tables and partitioned tables its series read. A source of a series
//! names its table by the place of the table among the figure's tables; a client that
//! fetches the figure is sent the description and, as the references of the message, the
//! tables in that order and the partitioned tables after them.
//!
//! The description is made by whoever plots: the `deephaven.plot` package of the Python
//! console (`console::python::figure`). Nothing here knows a plot style from another.
//! `docs/FIGURES.md` is the design.
//!
//! Behavior: `io.deephaven.plot.FigureWidget`, `io.deephaven.figure.FigureWidgetTypePlugin`,
//! and the part of `io.deephaven.figure.FigureWidgetTranslator#translateFigure` that
//! exports the tables

use std::sync::Arc;

use oxhaven_contracts::table::Table;
use oxhaven_proto::script::figure_descriptor::{ChartDescriptor, SeriesDescriptor};
use oxhaven_proto::script::FigureDescriptor;
use prost::Message;

use crate::object::types::{send_once, MessageStream, ObjectType};
use crate::partitioned::Partitioned;
use crate::session::{ExportError, ExportValue};

#[cfg(test)]
mod tests;

/// The name clients know the type of a figure by.
///
/// Behavior: `FigureWidgetTypePlugin#name`
pub const TYPE_NAME: &str = "Figure";

/// A figure that was shown: its description and what it draws from.
pub struct FigureWidget {
    descriptor: FigureDescriptor,
    tables: Vec<Arc<dyn Table>>,
    partitioned: Vec<Partitioned>,
}

/// A figure as it is held and exported.
pub type Figure = Arc<FigureWidget>;

impl FigureWidget {
    /// A figure of `descriptor`, whose sources name `tables` and `partitioned` by their
    /// places. Fails for a source that names a place there is nothing at, or a column its
    /// table does not have: a client would fail on either after it has drawn the rest.
    pub fn new(
        descriptor: FigureDescriptor,
        tables: Vec<Arc<dyn Table>>,
        partitioned: Vec<Partitioned>,
    ) -> Result<Self, String> {
        for chart in &descriptor.charts {
            check_chart(chart, &tables, &partitioned)?;
        }
        Ok(Self {
            descriptor,
            tables,
            partitioned,
        })
    }

    pub fn descriptor(&self) -> &FigureDescriptor {
        &self.descriptor
    }

    pub fn tables(&self) -> &[Arc<dyn Table>] {
        &self.tables
    }

    pub fn partitioned(&self) -> &[Partitioned] {
        &self.partitioned
    }
}

fn check_chart(
    chart: &ChartDescriptor,
    tables: &[Arc<dyn Table>],
    partitioned: &[Partitioned],
) -> Result<(), String> {
    let partitioned_at = |id: i32, series: &str| {
        usize::try_from(id)
            .ok()
            .and_then(|place| partitioned.get(place))
            .ok_or_else(|| format!("series {series}: there is no partitioned table {id}"))
    };
    for series in &chart.series {
        check_series(series, tables)?;
    }
    for series in &chart.multi_series {
        for source in &series.data_sources {
            let table = partitioned_at(source.partitioned_table_id, &series.name)?;
            let definition = table.constituent_definition();
            if definition.index_of(&source.column_name).is_none() {
                return Err(format!(
                    "series {}: the constituents have no column {}",
                    series.name, source.column_name
                ));
            }
        }
    }
    Ok(())
}

fn check_series(series: &SeriesDescriptor, tables: &[Arc<dyn Table>]) -> Result<(), String> {
    for source in &series.data_sources {
        // a source that Java could not describe names no column, and says so among the
        // figure's errors
        if source.column_name.is_empty() {
            continue;
        }
        let table = usize::try_from(source.table_id)
            .ok()
            .and_then(|place| tables.get(place))
            .ok_or_else(|| {
                format!(
                    "series {}: there is no table {}",
                    series.name, source.table_id
                )
            })?;
        if table.definition().index_of(&source.column_name).is_none() {
            return Err(format!(
                "series {}: table {} has no column {}",
                series.name, source.table_id, source.column_name
            ));
        }
    }
    Ok(())
}

/// The figure of a value, if it is one.
pub fn figure_of(value: &ExportValue) -> Option<&Figure> {
    value.downcast_ref::<Figure>()
}

/// A figure as a value to export.
pub fn export_value(figure: Figure) -> ExportValue {
    ExportValue::object(figure)
}

/// A figure, as clients open one: its description, and references to its tables and then
/// to its partitioned tables.
///
/// Behavior: `FigureWidgetTypePlugin`, a fetch-only type
pub(crate) struct FigureType;

impl ObjectType for FigureType {
    fn name(&self) -> &str {
        TYPE_NAME
    }

    fn is_type(&self, value: &ExportValue) -> Result<bool, ExportError> {
        Ok(figure_of(value).is_some())
    }

    fn connect(
        &self,
        value: &ExportValue,
        connection: Arc<dyn MessageStream>,
    ) -> Result<Arc<dyn MessageStream>, ExportError> {
        let figure = figure_of(value)
            .ok_or_else(|| ExportError::Internal("not a figure".to_string()))?;
        let tables = figure.tables.iter().cloned().map(ExportValue::Table);
        let partitioned = figure
            .partitioned
            .iter()
            .cloned()
            .map(crate::partitioned::export_value);
        send_once(
            connection.as_ref(),
            figure.descriptor.encode_to_vec(),
            tables.chain(partitioned).collect(),
        )
    }
}
