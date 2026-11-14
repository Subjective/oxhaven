//! Interface contracts shared by every Oxhaven crate.
//!
//! Types and traits here are frozen once approved. Change them only through a
//! contract-change pull request; see PORTING.md.

/// Marks a call into behavior that has not been ported yet.
///
/// Use this instead of guessing what the Java engine does. The message must
/// name the Java class and method that defines the missing behavior, so the
/// ledger can track it.
#[macro_export]
macro_rules! unported {
    ($java_ref:literal) => {
        panic!(concat!("unported: ", $java_ref))
    };
    ($java_ref:literal, $($arg:tt)+) => {
        panic!(concat!("unported: ", $java_ref, ": {}"), format_args!($($arg)+))
    };
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic(expected = "unported: io.deephaven.engine.rowset.RowSet#invert")]
    fn unported_names_the_java_source() {
        unported!("io.deephaven.engine.rowset.RowSet#invert");
    }
}
