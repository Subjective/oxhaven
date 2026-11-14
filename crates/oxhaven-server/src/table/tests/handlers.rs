//! The operator handlers, called as a client calls them.

use super::*;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::SortDescriptor;

mod asof;
mod update_by;

/// What the Java server answers for any exception out of the engine.
fn assert_internal(status: &Status) {
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(
        status.message().starts_with("Details Logged w/ID '"),
        "{status:?}"
    );
}

/// Calls the one-shot RPC of an operation.
async fn call(c: &mut Connected, op: Op) -> Result<ExportedTableCreationResponse, Status> {
    let response = match op {
        Op::EmptyTable(r) => c.client.empty_table(c.auth(r)).await,
        Op::TimeTable(r) => c.client.time_table(c.auth(r)).await,
        Op::Update(r) => c.client.update(c.auth(r)).await,
        Op::LazyUpdate(r) => c.client.lazy_update(c.auth(r)).await,
        Op::View(r) => c.client.view(c.auth(r)).await,
        Op::UpdateView(r) => c.client.update_view(c.auth(r)).await,
        Op::Select(r) => c.client.select(c.auth(r)).await,
        Op::DropColumns(r) => c.client.drop_columns(c.auth(r)).await,
        Op::Sort(r) => c.client.sort(c.auth(r)).await,
        Op::Head(r) => c.client.head(c.auth(r)).await,
        Op::Tail(r) => c.client.tail(c.auth(r)).await,
        Op::Slice(r) => c.client.slice(c.auth(r)).await,
        Op::Merge(r) => c.client.merge_tables(c.auth(r)).await,
        Op::Flatten(r) => c.client.flatten(c.auth(r)).await,
        Op::UnstructuredFilter(r) => c.client.unstructured_filter(c.auth(r)).await,
        Op::Filter(r) => c.client.filter(c.auth(r)).await,
        Op::Aggregate(r) => c.client.aggregate(c.auth(r)).await,
        Op::AggregateAll(r) => c.client.aggregate_all(c.auth(r)).await,
        Op::ComboAggregate(r) => c.client.combo_aggregate(c.auth(r)).await,
        Op::SelectDistinct(r) => c.client.select_distinct(c.auth(r)).await,
        Op::WhereIn(r) => c.client.where_in(c.auth(r)).await,
        Op::NaturalJoin(r) => c.client.natural_join_tables(c.auth(r)).await,
        Op::ExactJoin(r) => c.client.exact_join_tables(c.auth(r)).await,
        Op::LeftJoin(r) => c.client.left_join_tables(c.auth(r)).await,
        Op::Aj(r) => c.client.aj_tables(c.auth(r)).await,
        Op::Raj(r) => c.client.raj_tables(c.auth(r)).await,
        Op::AsOfJoin(r) => c.client.as_of_join_tables(c.auth(r)).await,
        Op::Snapshot(r) => c.client.snapshot(c.auth(r)).await,
        Op::SnapshotWhen(r) => c.client.snapshot_when(c.auth(r)).await,
        Op::UpdateBy(r) => c.client.update_by(c.auth(r)).await,
        other => panic!("no call for {}", op_name(&other)),
    };
    response.map(Response::into_inner)
}

fn formulas(source: i32, result: i32, columns: &[&str]) -> SelectOrUpdateRequest {
    SelectOrUpdateRequest {
        result_id: ticket(result),
        source_id: by_ticket(source),
        column_specs: columns.iter().map(|c| (*c).to_string()).collect(),
    }
}

/// Exports, as `id`, six rows with `I` = 0 to 5, `N` = 3 down to -2 and `S` = a, b, a, b,
/// a, b: six tables of one row of constants, merged.
async fn numbers(c: &mut Connected, id: i32) {
    let mut ops = Vec::new();
    let mut rows = Vec::new();
    for row in 0..6 {
        let letter = if row % 2 == 0 { "a" } else { "b" };
        let columns = [
            format!("I = {row}"),
            format!("N = {}", 3 - row),
            format!("S = `{letter}`"),
        ];
        ops.push(Op::EmptyTable(empty(None, 1)));
        ops.push(Op::Update(SelectOrUpdateRequest {
            result_id: None,
            source_id: by_offset(2 * row),
            column_specs: columns.to_vec(),
        }));
        rows.push(by_offset(2 * row + 1).unwrap());
    }
    ops.push(Op::Merge(MergeTablesRequest {
        result_id: ticket(id),
        source_ids: rows,
        key_column: String::new(),
    }));
    let (responses, status) = c.batch(ops).await.unwrap();
    assert!(status.is_none(), "{status:?}");
    assert!(responses.iter().all(|response| response.success));
}

fn column_names(table: &dyn Table) -> Vec<String> {
    let columns = table.definition().columns();
    columns.iter().map(|c| c.name.clone()).collect()
}

/// The values of a column in row order, as text.
fn values(table: &dyn Table, name: &str) -> Vec<String> {
    let index = table.definition().index_of(name).unwrap();
    let array = table.column(index).chunk(&table.row_set()).unwrap();
    // an instant prints as its nanoseconds
    let array = match array.data_type() {
        DataType::Timestamp(_, _) => cast(&array, &DataType::Int64).unwrap(),
        _ => array,
    };
    (0..array.len())
        .map(|row| array_value_to_string(&array, row).unwrap())
        .collect()
}

fn texts(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_string()).collect()
}

fn ints(values: &[i64]) -> Vec<String> {
    values.iter().map(i64::to_string).collect()
}

// ---------------------------------------------------------------- formulas

#[tokio::test]
async fn the_five_formula_operations_keep_or_replace_the_source_columns() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    type Wrap = fn(SelectOrUpdateRequest) -> Op;
    let keeping: [Wrap; 3] = [Op::Update, Op::LazyUpdate, Op::UpdateView];
    let replacing: [Wrap; 2] = [Op::View, Op::Select];
    let mut next = 10;
    for (wraps, expected) in [
        (&keeping[..], vec!["I", "N", "S", "Twice"]),
        (&replacing[..], vec!["Twice", "S"]),
    ] {
        for wrap in wraps {
            next += 1;
            let request = formulas(1, next, &["Twice = I * 2", "S"]);
            let response = call(&mut c, wrap(request)).await.unwrap();
            assert!(response.success);
            assert!(response.is_static);
            assert_eq!(response.size, 6);
            let table = c.table(next);
            assert_eq!(column_names(table.as_ref()), expected);
            assert_eq!(values(table.as_ref(), "Twice"), ints(&[0, 2, 4, 6, 8, 10]));
            let described: Vec<String> = schema_of(&response)
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(described, expected);
        }
    }
}

#[tokio::test]
async fn a_formula_that_does_not_compile_is_an_internal_error() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let mut next = 10;
    for wrap in [
        Op::Update,
        Op::LazyUpdate,
        Op::UpdateView,
        Op::View,
        Op::Select,
    ] {
        for bad in ["X = Missing + 1", "X = I +", "not an assignment", "i = 3"] {
            next += 1;
            let refused = call(&mut c, wrap(formulas(1, next, &[bad])))
                .await
                .unwrap_err();
            assert_internal(&refused);
        }
    }
}

/// PARITY.md, "Expressions the gRPC validator refuses".
#[tokio::test]
async fn expressions_the_java_validator_refuses_are_refused() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let mut next = 10;
    for refused in [
        "Math.abs(I)",
        "Math.max(I, N)",
        "java.lang.Math.sin(I)",
        "new java.net.URL(S)",
        "'PT1h'.multipliedBy(ii)",
    ] {
        for wrap in [Op::Update, Op::View] {
            next += 1;
            let assignment = format!("X = {refused}");
            let status = call(&mut c, wrap(formulas(1, next, &[&assignment])))
                .await
                .unwrap_err();
            assert_internal(&status);
        }
        next += 1;
        let filter = UnstructuredFilterTableRequest {
            result_id: ticket(next),
            source_id: by_ticket(1),
            filters: vec![format!("{refused} != null")],
        };
        let status = call(&mut c, Op::UnstructuredFilter(filter))
            .await
            .unwrap_err();
        assert_internal(&status);
    }
    // an expression without such a call is accepted
    let accepted = call(&mut c, Op::Update(formulas(1, 2, &["X = N * 2"])))
        .await
        .unwrap();
    assert!(accepted.success);
    assert_eq!(
        values(c.table(2).as_ref(), "X"),
        ints(&[6, 4, 2, 0, -2, -4])
    );
}

/// A filter whose regular expression the engine has not ported, which panics in the engine.
fn unported_filter(result: i32) -> FilterTableRequest {
    FilterTableRequest {
        result_id: ticket(result),
        source_id: by_ticket(1),
        filters: vec![condition(proto::condition::Data::Matches(
            proto::MatchesCondition {
                reference: Some(proto::Reference {
                    column_name: "S".to_string(),
                }),
                regex: "[a[b]]".to_string(),
                case_sensitivity: 0,
                match_type: 0,
            },
        ))],
    }
}

#[tokio::test]
async fn a_request_the_engine_has_not_ported_is_refused_by_name_and_the_server_goes_on() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let refused = call(&mut c, Op::Filter(unported_filter(2)))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "not ported: java.util.regex.Pattern#compile: a nested character class"
    );

    // In a batch the failure is this operation's and names the same thing.
    let (responses, status) = c.batch(vec![Op::Filter(unported_filter(3))]).await.unwrap();
    assert_eq!(status.unwrap().code(), Code::Unimplemented);
    assert!(!responses[0].success);
    assert!(
        responses[0]
            .error_info
            .contains("not ported: java.util.regex.Pattern#compile"),
        "{}",
        responses[0].error_info
    );

    let served = call(&mut c, Op::Update(formulas(1, 4, &["X = I"])))
        .await
        .unwrap();
    assert!(served.success);
}

#[tokio::test]
async fn an_update_of_a_refreshing_table_follows_it() {
    let mut c = Connected::start().await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    let response = call(&mut c, Op::Update(formulas(1, 2, &["X = 7"])))
        .await
        .unwrap();
    assert!(!response.is_static);
    let (parent, result) = (c.table(1), c.table(2));
    let graph = c.server.state.graph.clone();
    let sizes = || {
        locked(graph.as_ref(), || {
            (parent.row_set().size(), result.row_set().size())
        })
    };
    let (_, before) = sizes();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    let (parent_size, result_size) = sizes();
    assert!(result_size > before);
    assert_eq!(result_size, parent_size);
}

// ---------------------------------------------------------------- columns

#[tokio::test]
async fn drop_columns_removes_columns_and_leaves_a_missing_one_to_the_engine() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let drop = |result: i32, names: &[&str]| {
        Op::DropColumns(DropColumnsRequest {
            result_id: ticket(result),
            source_id: by_ticket(1),
            column_names: texts(names),
        })
    };
    let response = call(&mut c, drop(2, &["N", "I"])).await.unwrap();
    assert_eq!(response.size, 6);
    assert_eq!(column_names(c.table(2).as_ref()), ["S"]);
    assert_internal(&call(&mut c, drop(3, &["N", "Missing"])).await.unwrap_err());
}

// ---------------------------------------------------------------- sort

fn sort_by(direction: SortDirection, column: &str) -> SortDescriptor {
    SortDescriptor {
        column_name: column.to_string(),
        is_absolute: false,
        direction: direction as i32,
    }
}

fn absolute(mut descriptor: SortDescriptor) -> SortDescriptor {
    descriptor.is_absolute = true;
    descriptor
}

fn sorted(result: i32, sorts: Vec<SortDescriptor>) -> Op {
    Op::Sort(SortTableRequest {
        result_id: ticket(result),
        source_id: by_ticket(1),
        sorts,
    })
}

#[tokio::test]
async fn sort_orders_by_columns_absolute_values_and_reversal() {
    use SortDirection::{Ascending, Descending, Reverse};
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let cases: Vec<(Vec<SortDescriptor>, [i64; 6])> = vec![
        (
            vec![sort_by(Descending, "S"), sort_by(Descending, "I")],
            [5, 3, 1, 4, 2, 0],
        ),
        (vec![sort_by(Ascending, "N")], [5, 4, 3, 2, 1, 0]),
        // |N| is 3, 2, 1, 0, 1, 2 and equal rows keep their order
        (vec![absolute(sort_by(Ascending, "N"))], [3, 2, 4, 1, 5, 0]),
        (vec![absolute(sort_by(Descending, "N"))], [0, 1, 5, 2, 4, 3]),
        (vec![sort_by(Reverse, "")], [5, 4, 3, 2, 1, 0]),
        // S ascending and then, the table being reversed at the end, I ascending as asked:
        // the sort is by S ascending and I descending, reversed
        (
            vec![
                sort_by(Ascending, "S"),
                sort_by(Reverse, ""),
                sort_by(Ascending, "I"),
            ],
            [1, 3, 5, 0, 2, 4],
        ),
        // two reversals cancel
        (
            vec![
                sort_by(Reverse, ""),
                sort_by(Reverse, ""),
                sort_by(Descending, "I"),
            ],
            [5, 4, 3, 2, 1, 0],
        ),
        (
            vec![sort_by(Reverse, ""), sort_by(Reverse, "")],
            [0, 1, 2, 3, 4, 5],
        ),
        (vec![], [0, 1, 2, 3, 4, 5]),
    ];
    let mut next = 10;
    for (sorts, expected) in cases {
        next += 1;
        let response = call(&mut c, sorted(next, sorts.clone())).await.unwrap();
        assert_eq!(response.size, 6);
        assert_eq!(
            values(c.table(next).as_ref(), "I"),
            ints(&expected),
            "{sorts:?}"
        );
        assert_eq!(column_names(c.table(next).as_ref()), ["I", "N", "S"]);
    }
}

#[tokio::test]
async fn sort_refuses_as_java_does() {
    use SortDirection::{Ascending, Descending, Reverse, Unknown};
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;

    // a missing column is the handler's own check; a REVERSE descriptor names no column
    let sorts = vec![
        sort_by(Ascending, "Y"),
        sort_by(Reverse, "Ignored"),
        sort_by(Descending, "I"),
        sort_by(Descending, "X"),
    ];
    let refused = call(&mut c, sorted(2, sorts)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "column(s) not found: Y, X");

    let refused = call(&mut c, sorted(3, vec![sort_by(Unknown, "I")]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(refused.message(), "Unexpected sort direction: 0");

    // the missing column is found before the direction is looked at
    let refused = call(&mut c, sorted(4, vec![sort_by(Unknown, "Y")]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);

    // an empty name passes the column check and fails in the engine
    let refused = call(&mut c, sorted(5, vec![sort_by(Ascending, "")]))
        .await
        .unwrap_err();
    assert_internal(&refused);

    // abs of a string does not compile in Java, whether or not the column is sorted by
    for (result, direction) in [(6, Ascending), (8, Reverse)] {
        let refused = call(
            &mut c,
            sorted(result, vec![absolute(sort_by(direction, "S"))]),
        )
        .await
        .unwrap_err();
        assert_internal(&refused);
    }
    // and of a number it does
    let accepted = call(&mut c, sorted(7, vec![absolute(sort_by(Reverse, "N"))]))
        .await
        .unwrap();
    assert_eq!(accepted.size, 6);
}

// ---------------------------------------------------------------- head, tail, slice

fn rows(result: i32, num_rows: i64) -> HeadOrTailRequest {
    HeadOrTailRequest {
        result_id: ticket(result),
        source_id: by_ticket(1),
        num_rows,
    }
}

#[tokio::test]
async fn head_and_tail_take_rows_from_either_end() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    call(&mut c, Op::Head(rows(2, 2))).await.unwrap();
    assert_eq!(values(c.table(2).as_ref(), "I"), ints(&[0, 1]));
    call(&mut c, Op::Tail(rows(3, 2))).await.unwrap();
    assert_eq!(values(c.table(3).as_ref(), "I"), ints(&[4, 5]));
    let none = call(&mut c, Op::Head(rows(4, 0))).await.unwrap();
    assert_eq!(none.size, 0);
    let all = call(&mut c, Op::Tail(rows(5, 100))).await.unwrap();
    assert_eq!(all.size, 6);

    for wrap in [Op::Head, Op::Tail] {
        let refused = call(&mut c, wrap(rows(6, -3))).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert_eq!(refused.message(), "numRows must be >= 0 (found: -3)");
    }
    // a refused request does not use up its ticket
    call(&mut c, Op::Head(rows(6, 1))).await.unwrap();
}

#[tokio::test]
async fn slice_takes_rows_between_positions_from_either_end() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let slice = |result: i32, first: i64, last: i64| {
        Op::Slice(SliceRequest {
            result_id: ticket(result),
            source_id: by_ticket(1),
            first_position_inclusive: first,
            last_position_exclusive: last,
        })
    };
    call(&mut c, slice(2, 1, 3)).await.unwrap();
    assert_eq!(values(c.table(2).as_ref(), "I"), ints(&[1, 2]));
    call(&mut c, slice(3, -3, -1)).await.unwrap();
    assert_eq!(values(c.table(3).as_ref(), "I"), ints(&[3, 4]));
    call(&mut c, slice(4, 2, -2)).await.unwrap();
    assert_eq!(values(c.table(4).as_ref(), "I"), ints(&[2, 3]));

    let refused = call(&mut c, slice(5, 3, 1)).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(
        refused.message(),
        "Cannot slice with a non-negative start position that is after a non-negative end position."
    );
    let refused = call(&mut c, slice(5, -1, -3)).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(
        refused.message(),
        "Cannot slice with a negative start position that is after a negative end position."
    );
    let no_source = Op::Slice(SliceRequest {
        result_id: ticket(5),
        source_id: None,
        first_position_inclusive: 0,
        last_position_exclusive: 1,
    });
    let refused = call(&mut c, no_source).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(
        refused.message(),
        "io.deephaven.proto.backplane.grpc.SliceRequest must have field source_id (2)"
    );
}

// ---------------------------------------------------------------- merge, flatten

fn merged(result: i32, sources: &[i32], key_column: &str) -> Op {
    Op::Merge(MergeTablesRequest {
        result_id: ticket(result),
        source_ids: sources.iter().filter_map(|id| by_ticket(*id)).collect(),
        key_column: key_column.to_string(),
    })
}

#[tokio::test]
async fn merge_appends_tables_of_the_same_columns() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    call(&mut c, Op::Head(rows(2, 2))).await.unwrap();
    let response = call(&mut c, merged(3, &[2, 1, 2], "")).await.unwrap();
    assert_eq!(response.size, 10);
    assert!(response.is_static);
    assert_eq!(
        values(c.table(3).as_ref(), "I"),
        ints(&[0, 1, 0, 1, 2, 3, 4, 5, 0, 1])
    );

    let refused = call(&mut c, merged(4, &[], "")).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(refused.message(), "Cannot merge zero source tables.");

    // different columns: the engine's refusal
    call(&mut c, Op::EmptyTable(empty(ticket(5), 1)))
        .await
        .unwrap();
    assert_internal(&call(&mut c, merged(6, &[1, 5], "")).await.unwrap_err());

    // Java's sorted merge of static tables is not ported
    let refused = call(&mut c, merged(7, &[1, 2], "I")).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "not ported: io.deephaven.engine.util.TableTools#mergeSorted: a merge of static tables with a key column"
    );
}

#[tokio::test]
async fn a_merge_of_refreshing_tables_with_a_key_column_is_sorted_by_it() {
    let mut c = Connected::start().await;
    // one row each; the second started earlier
    let mut later = one_row_time_table(ticket(1));
    later.start_time = Some(StartTime::StartTimeNanos(1_000));
    call(&mut c, Op::TimeTable(later)).await.unwrap();
    call(&mut c, Op::TimeTable(one_row_time_table(ticket(2))))
        .await
        .unwrap();

    let plain = call(&mut c, merged(3, &[1, 2], "")).await.unwrap();
    assert!(!plain.is_static);
    let keyed = call(&mut c, merged(4, &[1, 2], "Timestamp")).await.unwrap();
    assert!(!keyed.is_static);
    let unsorted = values(c.table(3).as_ref(), "Timestamp");
    let sorted = values(c.table(4).as_ref(), "Timestamp");
    assert_eq!(unsorted.len(), 2);
    assert_ne!(unsorted[0], unsorted[1]);
    assert_eq!(sorted, vec![unsorted[1].clone(), unsorted[0].clone()]);

    assert_internal(
        &call(&mut c, merged(5, &[1, 2], "Missing"))
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn flatten_gives_the_rows_the_keys_of_their_positions() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let filter = UnstructuredFilterTableRequest {
        result_id: ticket(2),
        source_id: by_ticket(1),
        filters: vec!["I >= 3".to_string()],
    };
    call(&mut c, Op::UnstructuredFilter(filter)).await.unwrap();
    assert_ne!(*c.table(2).row_set(), RowSet::flat(3));

    let request = FlattenRequest {
        result_id: ticket(3),
        source_id: by_ticket(2),
    };
    let response = call(&mut c, Op::Flatten(request)).await.unwrap();
    assert_eq!(response.size, 3);
    assert_eq!(*c.table(3).row_set(), RowSet::flat(3));
    assert_eq!(values(c.table(3).as_ref(), "I"), ints(&[3, 4, 5]));
}

// ---------------------------------------------------------------- filters

#[tokio::test]
async fn unstructured_filter_keeps_the_rows_every_filter_accepts() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let filter = |result: i32, filters: &[&str]| {
        Op::UnstructuredFilter(UnstructuredFilterTableRequest {
            result_id: ticket(result),
            source_id: by_ticket(1),
            filters: texts(filters),
        })
    };
    let response = call(&mut c, filter(2, &["I > 1", "S == `a`"]))
        .await
        .unwrap();
    assert_eq!(response.size, 2);
    assert!(response.is_static);
    assert_eq!(values(c.table(2).as_ref(), "I"), ints(&[2, 4]));
    // a formula
    call(&mut c, filter(3, &["I % 2 == 1 && N < 2"]))
        .await
        .unwrap();
    assert_eq!(values(c.table(3).as_ref(), "I"), ints(&[3, 5]));
    let all = call(&mut c, filter(4, &[])).await.unwrap();
    assert_eq!(all.size, 6);

    for (result, bad) in [(5, "Missing > 1"), (6, "I +"), (7, "I + 1")] {
        assert_internal(&call(&mut c, filter(result, &[bad])).await.unwrap_err());
    }
}

fn reference(column: &str) -> proto::Value {
    proto::Value {
        data: Some(proto::value::Data::Reference(proto::Reference {
            column_name: column.to_string(),
        })),
    }
}

fn literal(value: proto::literal::Value) -> proto::Value {
    proto::Value {
        data: Some(proto::value::Data::Literal(proto::Literal {
            value: Some(value),
        })),
    }
}

fn condition(data: proto::condition::Data) -> proto::Condition {
    proto::Condition { data: Some(data) }
}

fn compare(
    operation: proto::compare_condition::CompareOperation,
    lhs: proto::Value,
    rhs: proto::Value,
) -> proto::Condition {
    condition(proto::condition::Data::Compare(proto::CompareCondition {
        operation: operation as i32,
        case_sensitivity: 0,
        lhs: Some(lhs),
        rhs: Some(rhs),
    }))
}

#[tokio::test]
async fn filter_keeps_the_rows_every_condition_accepts() {
    use proto::compare_condition::CompareOperation;
    use proto::condition::Data;
    use proto::literal::Value as Lit;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let filter = |result: i32, filters: Vec<proto::Condition>| {
        Op::Filter(FilterTableRequest {
            result_id: ticket(result),
            source_id: by_ticket(1),
            filters,
        })
    };
    let at_least_two = compare(
        CompareOperation::GreaterThanOrEqual,
        reference("I"),
        literal(Lit::LongValue(2)),
    );
    let is_a = condition(Data::In(proto::InCondition {
        target: Some(reference("S")),
        candidates: vec![literal(Lit::StringValue("a".to_string()))],
        case_sensitivity: 0,
        match_type: 0,
        nan_comparison: 0,
    }));

    // the conditions of a request are joined by AND
    let response = call(&mut c, filter(2, vec![at_least_two.clone(), is_a.clone()]))
        .await
        .unwrap();
    assert_eq!(response.size, 2);
    assert_eq!(values(c.table(2).as_ref(), "I"), ints(&[2, 4]));

    let either = condition(Data::Or(proto::OrCondition {
        filters: vec![
            condition(Data::Not(Box::new(proto::NotCondition {
                filter: Some(Box::new(at_least_two.clone())),
            }))),
            compare(
                CompareOperation::Equals,
                reference("N"),
                literal(Lit::LongValue(-2)),
            ),
        ],
    }));
    call(&mut c, filter(3, vec![either])).await.unwrap();
    assert_eq!(values(c.table(3).as_ref(), "I"), ints(&[0, 1, 5]));

    let contains = condition(Data::Contains(proto::ContainsCondition {
        reference: Some(proto::Reference {
            column_name: "S".to_string(),
        }),
        search_string: "B".to_string(),
        case_sensitivity: proto::CaseSensitivity::IgnoreCase as i32,
        match_type: 0,
    }));
    call(&mut c, filter(4, vec![contains])).await.unwrap();
    assert_eq!(values(c.table(4).as_ref(), "I"), ints(&[1, 3, 5]));

    // no conditions: the source itself
    let all = call(&mut c, filter(5, vec![])).await.unwrap();
    assert_eq!(all.size, 6);
    assert!(Arc::ptr_eq(&c.table(5), &c.table(1)));
}

#[tokio::test]
async fn a_condition_with_a_part_that_is_not_set_is_an_internal_error() {
    use proto::compare_condition::CompareOperation;
    use proto::condition::Data;
    use proto::literal::Value as Lit;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let nothing = proto::Value { data: None };
    let no_literal = proto::Value {
        data: Some(proto::value::Data::Literal(proto::Literal { value: None })),
    };
    let malformed = vec![
        proto::Condition { data: None },
        condition(Data::Not(Box::new(proto::NotCondition { filter: None }))),
        compare(CompareOperation::LessThan, reference("I"), nothing),
        compare(CompareOperation::LessThan, reference("I"), no_literal),
        condition(Data::Compare(proto::CompareCondition {
            operation: 0,
            case_sensitivity: 0,
            lhs: Some(reference("I")),
            rhs: None,
        })),
        condition(Data::IsNan(Default::default())),
        // well formed, of a column that does not exist
        compare(
            CompareOperation::LessThan,
            reference("Missing"),
            literal(Lit::LongValue(1)),
        ),
    ];
    for (index, bad) in malformed.into_iter().enumerate() {
        let request = FilterTableRequest {
            result_id: ticket(10 + index as i32),
            source_id: by_ticket(1),
            filters: vec![bad.clone()],
        };
        let refused = call(&mut c, Op::Filter(request)).await.unwrap_err();
        assert_internal(&refused);
    }
}

// ---------------------------------------------------------------- aggregation

fn agg_spec(kind: proto::agg_spec::Type) -> proto::AggSpec {
    proto::AggSpec { r#type: Some(kind) }
}

fn agg_columns(kind: proto::agg_spec::Type, pairs: &[&str]) -> proto::Aggregation {
    proto::Aggregation {
        r#type: Some(proto::aggregation::Type::Columns(
            proto::aggregation::AggregationColumns {
                spec: Some(agg_spec(kind)),
                match_pairs: texts(pairs),
            },
        )),
    }
}

fn agg_count(column: &str) -> proto::Aggregation {
    proto::Aggregation {
        r#type: Some(proto::aggregation::Type::Count(
            proto::aggregation::AggregationCount {
                column_name: column.to_string(),
            },
        )),
    }
}

fn aggregated(result: i32, aggregations: Vec<proto::Aggregation>, group_by: &[&str]) -> Op {
    Op::Aggregate(AggregateRequest {
        result_id: ticket(result),
        source_id: by_ticket(1),
        initial_groups_id: None,
        preserve_empty: false,
        aggregations,
        group_by_columns: texts(group_by),
    })
}

#[tokio::test]
async fn aggregate_groups_rows_and_aggregates_columns() {
    use proto::agg_spec::Type;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let request = aggregated(
        2,
        vec![
            agg_columns(Type::Sum(Default::default()), &["Total = N", "I"]),
            agg_count("Rows"),
        ],
        &["S"],
    );
    let response = call(&mut c, request).await.unwrap();
    assert_eq!(response.size, 2);
    assert!(response.is_static);
    let table = c.table(2);
    assert_eq!(column_names(table.as_ref()), ["S", "Total", "I", "Rows"]);
    assert_eq!(values(table.as_ref(), "S"), texts(&["a", "b"]));
    assert_eq!(values(table.as_ref(), "Total"), ints(&[3, 0]));
    assert_eq!(values(table.as_ref(), "I"), ints(&[6, 9]));
    assert_eq!(values(table.as_ref(), "Rows"), ints(&[3, 3]));

    // every column that is not grouped by
    let all = Op::AggregateAll(AggregateAllRequest {
        result_id: ticket(3),
        source_id: by_ticket(1),
        spec: Some(agg_spec(Type::Max(Default::default()))),
        group_by_columns: texts(&["S"]),
    });
    call(&mut c, all).await.unwrap();
    let table = c.table(3);
    assert_eq!(column_names(table.as_ref()), ["S", "I", "N"]);
    assert_eq!(values(table.as_ref(), "I"), ints(&[4, 5]));
    assert_eq!(values(table.as_ref(), "N"), ints(&[3, 2]));
}

fn unique(
    sentinel: Option<proto::agg_spec::agg_spec_non_unique_sentinel::Type>,
) -> proto::agg_spec::Type {
    proto::agg_spec::Type::Unique(proto::agg_spec::AggSpecUnique {
        include_nulls: false,
        non_unique_sentinel: Some(proto::agg_spec::AggSpecNonUniqueSentinel { r#type: sentinel }),
    })
}

#[tokio::test]
async fn aggregate_has_the_specs_beyond_the_basic_set() {
    use proto::agg_spec::agg_spec_non_unique_sentinel::Type as Sentinel;
    use proto::agg_spec::{AggSpecSorted, AggSpecSortedColumn, AggSpecWeighted, Type};
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let weighted = |column: &str| AggSpecWeighted {
        weight_column: column.to_string(),
    };
    let by_n = AggSpecSorted {
        columns: vec![AggSpecSortedColumn {
            column_name: "N".to_string(),
        }],
    };
    // S = a has I = 0, 2, 4 and N = 3, 1, -1; S = b has I = 1, 3, 5 and N = 2, 0, -2.
    let request = aggregated(
        2,
        vec![
            agg_columns(Type::AbsSum(Default::default()), &["A = N"]),
            agg_columns(Type::Var(Default::default()), &["V = I"]),
            agg_columns(Type::Std(Default::default()), &["SD = I"]),
            agg_columns(Type::Median(Default::default()), &["M = I"]),
            agg_columns(
                Type::Percentile(proto::agg_spec::AggSpecPercentile {
                    percentile: 0.0,
                    average_evenly_divided: false,
                }),
                &["P = N"],
            ),
            agg_columns(Type::CountDistinct(Default::default()), &["CD = N"]),
            agg_columns(unique(Some(Sentinel::NullValue(0))), &["U = I"]),
            agg_columns(Type::WeightedSum(weighted("N")), &["WS = I"]),
            agg_columns(Type::WeightedAvg(weighted("I")), &["WA = N"]),
            agg_columns(Type::SortedFirst(by_n.clone()), &["SF = I"]),
            agg_columns(Type::SortedLast(by_n), &["SL = I"]),
        ],
        &["S"],
    );
    call(&mut c, request).await.unwrap();
    let table = c.table(2);
    let column = |name: &str| values(table.as_ref(), name);
    assert_eq!(column("A"), ints(&[5, 4]));
    assert_eq!(column("V"), texts(&["4.0", "4.0"]));
    assert_eq!(column("SD"), texts(&["2.0", "2.0"]));
    assert_eq!(column("M"), ints(&[2, 3]));
    assert_eq!(column("P"), ints(&[-1, -2]));
    assert_eq!(column("CD"), ints(&[3, 3]));
    assert_eq!(column("U"), texts(&["", ""]));
    // the weight and the value are integers: a long sum
    assert_eq!(column("WS"), ints(&[-2, -8]));
    // (3 * 0 + 1 * 2 - 1 * 4) / 6 and (2 * 1 + 0 * 3 - 2 * 5) / 9
    assert_eq!(
        column("WA"),
        texts(&["-0.3333333333333333", "-0.8888888888888888"])
    );
    assert_eq!(column("SF"), ints(&[4, 5]));
    assert_eq!(column("SL"), ints(&[0, 1]));

    // every column but the group-by columns and the weight
    let all = Op::AggregateAll(AggregateAllRequest {
        result_id: ticket(3),
        source_id: by_ticket(1),
        spec: Some(agg_spec(Type::WeightedSum(weighted("N")))),
        group_by_columns: texts(&["S"]),
    });
    call(&mut c, all).await.unwrap();
    let table = c.table(3);
    assert_eq!(column_names(table.as_ref()), ["S", "I"]);
    assert_eq!(values(table.as_ref(), "I"), ints(&[-2, -8]));
}

fn combo(
    result: i32,
    aggregates: Vec<proto::combo_aggregate_request::Aggregate>,
    group_by: &[&str],
    force_combo: bool,
) -> Op {
    Op::ComboAggregate(proto::ComboAggregateRequest {
        result_id: ticket(result),
        source_id: by_ticket(1),
        aggregates,
        group_by_columns: texts(group_by),
        force_combo,
    })
}

fn combo_part(
    kind: proto::combo_aggregate_request::AggType,
    pairs: &[&str],
    column: &str,
) -> proto::combo_aggregate_request::Aggregate {
    proto::combo_aggregate_request::Aggregate {
        r#type: kind as i32,
        match_pairs: texts(pairs),
        column_name: column.to_string(),
        percentile: 0.0,
        avg_median: false,
    }
}

#[tokio::test]
async fn combo_aggregate_runs_one_aggregation_by_itself_or_several_together() {
    use proto::combo_aggregate_request::AggType;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;

    // one aggregate, the table operation of its own: every column but S
    call(
        &mut c,
        combo(2, vec![combo_part(AggType::Median, &[], "")], &["S"], false),
    )
    .await
    .unwrap();
    let table = c.table(2);
    assert_eq!(column_names(table.as_ref()), ["S", "I", "N"]);
    // AggSpec#median() averages: an int column gives a double
    assert_eq!(values(table.as_ref(), "I"), texts(&["2.0", "3.0"]));

    // several, with match pairs, a count, a weighted average of every other column and a
    // percentile
    let mut percentile = combo_part(AggType::Percentile, &["P = N"], "");
    percentile.percentile = 0.5;
    percentile.avg_median = true;
    let request = combo(
        3,
        vec![
            combo_part(AggType::Sum, &["Total = N"], ""),
            combo_part(AggType::Count, &[], "Rows"),
            combo_part(AggType::WeightedAvg, &[], "I"),
            percentile,
        ],
        &["S"],
        false,
    );
    call(&mut c, request).await.unwrap();
    let table = c.table(3);
    assert_eq!(
        column_names(table.as_ref()),
        ["S", "Total", "Rows", "N", "P"]
    );
    assert_eq!(values(table.as_ref(), "Total"), ints(&[3, 0]));
    assert_eq!(values(table.as_ref(), "Rows"), ints(&[3, 3]));
    assert_eq!(
        values(table.as_ref(), "N"),
        texts(&["-0.3333333333333333", "-0.8888888888888888"])
    );
    assert_eq!(values(table.as_ref(), "P"), texts(&["1.0", "0.0"]));

    // force_combo: one aggregate, but as aggBy
    let request = combo(
        4,
        vec![combo_part(AggType::Max, &["Top = I"], "")],
        &["S"],
        true,
    );
    call(&mut c, request).await.unwrap();
    assert_eq!(column_names(c.table(4).as_ref()), ["S", "Top"]);
}

#[tokio::test]
async fn combo_aggregate_refuses_what_java_refuses() {
    use proto::combo_aggregate_request::AggType;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let with = |kind: AggType, edit: &dyn Fn(&mut proto::combo_aggregate_request::Aggregate)| {
        let mut part = combo_part(kind, &[], "");
        edit(&mut part);
        part
    };
    let cases: Vec<(
        Vec<proto::combo_aggregate_request::Aggregate>,
        &[&str],
        bool,
        &str,
    )> = vec![
        (
            vec![],
            &["S"],
            false,
            "ComboAggregateRequest incorrectly has zero aggregates provided",
        ),
        (
            vec![combo_part(AggType::Sum, &[], "")],
            &["bad name"],
            false,
            "ComboAggregateRequest group by",
        ),
        (
            vec![with(AggType::Sum, &|p| p.avg_median = true)],
            &["S"],
            false,
            "force_combo is false and only one aggregate provided, but avg_median is specified",
        ),
        (
            vec![with(AggType::Sum, &|p| p.percentile = 0.5)],
            &["S"],
            false,
            "force_combo is false and only one aggregate provided, but percentile is specified",
        ),
        (
            vec![with(AggType::Sum, &|p| p.percentile = 0.5)],
            &["S"],
            true,
            "percentile is specified for type SUM",
        ),
        (
            vec![with(AggType::Max, &|p| p.avg_median = true)],
            &["S"],
            true,
            "avg_median is specified for type MAX",
        ),
        (
            vec![combo_part(AggType::Count, &["I"], "Rows")],
            &["S"],
            true,
            "match_pairs is specified for type COUNT",
        ),
        (
            vec![combo_part(AggType::Min, &["I"], "W")],
            &["S"],
            true,
            "column_name is specified for type MIN",
        ),
    ];
    for (index, (aggregates, group_by, force, message)) in cases.into_iter().enumerate() {
        let refused = call(&mut c, combo(2 + index as i32, aggregates, group_by, force))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);
    }

    // what Java refuses with an exception: COUNT by itself, whose column name must be empty
    // to be by itself; a type it does not know; no column left to aggregate
    let unknown = proto::combo_aggregate_request::Aggregate {
        r#type: 99,
        ..combo_part(AggType::Sum, &[], "")
    };
    for (result, aggregates, group_by) in [
        (30, vec![combo_part(AggType::Count, &[], "")], &["S"][..]),
        (31, vec![unknown], &["S"]),
        (
            32,
            vec![
                combo_part(AggType::Sum, &[], ""),
                combo_part(AggType::Count, &[], "N"),
            ],
            &["I", "N", "S"],
        ),
    ] {
        assert_internal(
            &call(&mut c, combo(result, aggregates, group_by, false))
                .await
                .unwrap_err(),
        );
    }
}

#[tokio::test]
async fn aggregate_refuses_the_options_java_refuses() {
    use proto::agg_spec::agg_spec_non_unique_sentinel::Type as Sentinel;
    use proto::agg_spec::{AggSpecWeighted, Type};
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;

    let no_sentinel = Type::Unique(proto::agg_spec::AggSpecUnique {
        include_nulls: false,
        non_unique_sentinel: None,
    });
    let refusals = [
        (
            no_sentinel,
            "io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique must have field non_unique_sentinel (2)",
        ),
        (
            unique(None),
            "io.deephaven.proto.backplane.grpc.AggSpec.AggSpecNonUniqueSentinel must have oneof type. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.",
        ),
        (
            unique(Some(Sentinel::NullValue(1))),
            "AggSpecNonUniqueSentinel null_value out of range",
        ),
        (
            unique(Some(Sentinel::ByteValue(128))),
            "AggSpecNonUniqueSentinel byte_value out of range",
        ),
        (
            unique(Some(Sentinel::ShortValue(-32769))),
            "AggSpecNonUniqueSentinel short_value out of range",
        ),
        (
            unique(Some(Sentinel::CharValue(-1))),
            "AggSpecNonUniqueSentinel char_value out of range",
        ),
    ];
    for (index, (kind, message)) in refusals.into_iter().enumerate() {
        let request = aggregated(
            2 + index as i32,
            vec![agg_columns(kind, &["U = I"])],
            &["S"],
        );
        let refused = call(&mut c, request).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);
    }
    // in range
    for (result, sentinel) in [
        (10, Sentinel::ByteValue(-128)),
        (11, Sentinel::CharValue(65535)),
    ] {
        let request = aggregated(
            result,
            vec![agg_columns(unique(Some(sentinel)), &["U = I"])],
            &["S"],
        );
        let refused = call(&mut c, request).await.unwrap_err();
        assert_eq!(refused.code(), Code::Unimplemented, "{refused:?}");
    }

    // what the engine refuses
    let weight = |column: &str| {
        Type::WeightedAvg(AggSpecWeighted {
            weight_column: column.to_string(),
        })
    };
    let percentile = Type::Percentile(proto::agg_spec::AggSpecPercentile {
        percentile: 1.5,
        average_evenly_divided: false,
    });
    for (result, kind) in [
        (20, percentile),
        (21, weight("Missing")),
        (22, weight("S")),
        (23, Type::Var(Default::default())),
    ] {
        let pairs: &[&str] = if result == 23 { &["V = S"] } else { &["X = I"] };
        let request = aggregated(result, vec![agg_columns(kind, pairs)], &[]);
        assert_internal(&call(&mut c, request).await.unwrap_err());
    }
    let all = Op::AggregateAll(AggregateAllRequest {
        result_id: ticket(30),
        source_id: by_ticket(1),
        spec: Some(agg_spec(weight("Missing"))),
        group_by_columns: texts(&["S"]),
    });
    assert_internal(&call(&mut c, all).await.unwrap_err());
}

#[tokio::test]
async fn aggregate_refuses_what_java_refuses_and_names_what_is_not_ported() {
    use proto::agg_spec::Type;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let sum = || Type::Sum(Default::default());

    // malformed requests, with Java's messages
    let refused = call(&mut c, aggregated(2, vec![], &["S"]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(
        refused.message(),
        "io.deephaven.proto.backplane.grpc.AggregateRequest must have at least one aggregations (5)"
    );
    let no_type = proto::Aggregation { r#type: None };
    let refused = call(&mut c, aggregated(2, vec![no_type], &["S"]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert!(
        refused
            .message()
            .starts_with("io.deephaven.proto.backplane.grpc.Aggregation must have oneof type. "),
        "{refused:?}"
    );
    let refused = call(&mut c, aggregated(2, vec![agg_columns(sum(), &[])], &["S"]))
        .await
        .unwrap_err();
    assert_eq!(
        refused.message(),
        "io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns must have at least one match_pairs (2)"
    );
    let no_spec = Op::AggregateAll(AggregateAllRequest {
        result_id: ticket(2),
        source_id: by_ticket(1),
        spec: None,
        group_by_columns: Vec::new(),
    });
    let refused = call(&mut c, no_spec).await.unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert_eq!(
        refused.message(),
        "io.deephaven.proto.backplane.grpc.AggregateAllRequest must have field spec (3)"
    );

    // the engine's refusals
    for (result, pairs, group_by) in [
        (10, ["Missing"], "S"),
        (11, ["N"], "Missing"),
        (12, ["S"], "I"),
        (13, ["bad name = N"], "S"),
    ] {
        let request = aggregated(result, vec![agg_columns(sum(), &pairs)], &[group_by]);
        assert_internal(&call(&mut c, request).await.unwrap_err());
    }

    // an output with the name of a group-by column panics in the engine: refused before
    for (result, aggregation) in [(20, agg_columns(sum(), &["S = N"])), (21, agg_count("S"))] {
        let refused = call(&mut c, aggregated(result, vec![aggregation], &["S"]))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::Unimplemented);
        assert_eq!(
            refused.message(),
            "not ported: io.deephaven.engine.table.impl.by.AggregationContext#getResultColumns: aggregation output S has the name of a group-by column"
        );
    }
    let all_by_itself = Op::AggregateAll(AggregateAllRequest {
        result_id: ticket(22),
        source_id: by_ticket(1),
        spec: Some(agg_spec(Type::Distinct(Default::default()))),
        group_by_columns: texts(&["S"]),
    });
    let refused = call(&mut c, all_by_itself).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "not ported: io.deephaven.server.table.ops.AggSpecAdapter#adapt: the aggregation distinct"
    );
    let row_key = proto::Aggregation {
        r#type: Some(proto::aggregation::Type::FirstRowKey(Default::default())),
    };
    let refused = call(&mut c, aggregated(23, vec![row_key], &["S"]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    let mut with_groups = aggregated(24, vec![agg_count("Rows")], &["S"]);
    if let Op::Aggregate(request) = &mut with_groups {
        request.initial_groups_id = by_ticket(1);
    }
    let refused = call(&mut c, with_groups).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);

    // and the server goes on
    let served = call(&mut c, aggregated(30, vec![agg_count("Rows")], &[]))
        .await
        .unwrap();
    assert_eq!(values(c.table(30).as_ref(), "Rows"), ints(&[6]));
    assert_eq!(served.size, 1);
}

/// The columns of a table as `name: type`.
fn column_types(table: &dyn Table) -> Vec<String> {
    let columns = table.definition().columns();
    columns
        .iter()
        .map(|c| format!("{}: {:?}", c.name, c.column_type))
        .collect()
}

#[tokio::test]
async fn group_gives_vectors_through_each_of_the_three_requests() {
    use proto::agg_spec::Type;
    use proto::combo_aggregate_request::AggType;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let group = || Type::Group(Default::default());

    // Aggregate: among other aggregations, in the order given
    let request = aggregated(
        2,
        vec![
            agg_columns(group(), &["Is = I"]),
            agg_count("Rows"),
            agg_columns(group(), &["N"]),
        ],
        &["S"],
    );
    let response = call(&mut c, request).await.unwrap();
    assert_eq!(response.size, 2);
    let table = c.table(2);
    assert_eq!(
        column_types(table.as_ref()),
        [
            "S: String",
            "Is: Vector(Int)",
            "Rows: Long",
            "N: Vector(Int)"
        ]
    );
    assert_eq!(
        values(table.as_ref(), "Is"),
        texts(&["[0, 2, 4]", "[1, 3, 5]"])
    );
    assert_eq!(
        values(table.as_ref(), "N"),
        texts(&["[3, 1, -1]", "[2, 0, -2]"])
    );

    // AggregateAll, and ComboAggregate by itself, which is Table#groupBy: every column
    // that is not grouped by
    let all = Op::AggregateAll(AggregateAllRequest {
        result_id: ticket(3),
        source_id: by_ticket(1),
        spec: Some(agg_spec(group())),
        group_by_columns: texts(&["S"]),
    });
    call(&mut c, all).await.unwrap();
    let by_itself = combo(4, vec![combo_part(AggType::Group, &[], "")], &["S"], false);
    call(&mut c, by_itself).await.unwrap();
    for result in [3, 4] {
        let table = c.table(result);
        assert_eq!(
            column_types(table.as_ref()),
            ["S: String", "I: Vector(Int)", "N: Vector(Int)"]
        );
        assert_eq!(
            values(table.as_ref(), "I"),
            texts(&["[0, 2, 4]", "[1, 3, 5]"])
        );
    }

    // without keys, and with nothing left to group
    call(
        &mut c,
        combo(5, vec![combo_part(AggType::Group, &[], "")], &[], false),
    )
    .await
    .unwrap();
    let table = c.table(5);
    assert_eq!(
        column_types(table.as_ref()),
        ["I: Vector(Int)", "N: Vector(Int)", "S: Vector(String)"]
    );
    assert_eq!(values(table.as_ref(), "S"), texts(&["[a, b, a, b, a, b]"]));
    let nothing = combo(
        6,
        vec![combo_part(AggType::Group, &[], "")],
        &["I", "N", "S"],
        false,
    );
    assert_internal(&call(&mut c, nothing).await.unwrap_err());

    // ComboAggregate with other aggregates, and with match pairs
    let request = combo(
        7,
        vec![
            combo_part(AggType::Group, &["Ns = N"], ""),
            combo_part(AggType::Sum, &["Total = N"], ""),
        ],
        &["S"],
        false,
    );
    call(&mut c, request).await.unwrap();
    let table = c.table(7);
    assert_eq!(column_names(table.as_ref()), ["S", "Ns", "Total"]);
    assert_eq!(
        values(table.as_ref(), "Ns"),
        texts(&["[3, 1, -1]", "[2, 0, -2]"])
    );
    assert_eq!(values(table.as_ref(), "Total"), ints(&[3, 0]));
}

#[tokio::test]
async fn aggregate_keeps_the_row_of_an_empty_table_only_when_asked() {
    let mut c = Connected::start().await;
    numbers(&mut c, 2).await;
    call(
        &mut c,
        Op::Head(HeadOrTailRequest {
            result_id: ticket(1),
            source_id: by_ticket(2),
            num_rows: 0,
        }),
    )
    .await
    .unwrap();
    let dropped = call(&mut c, aggregated(3, vec![agg_count("Rows")], &[]))
        .await
        .unwrap();
    assert_eq!(dropped.size, 0);
    let mut preserving = aggregated(4, vec![agg_count("Rows")], &[]);
    if let Op::Aggregate(request) = &mut preserving {
        request.preserve_empty = true;
    }
    let kept = call(&mut c, preserving).await.unwrap();
    assert_eq!(kept.size, 1);
    assert_eq!(values(c.table(4).as_ref(), "Rows"), ints(&[0]));
}

// ---------------------------------------------------------------- select_distinct

fn distinct(result: i32, columns: &[&str]) -> Op {
    Op::SelectDistinct(proto::SelectDistinctRequest {
        result_id: ticket(result),
        source_id: by_ticket(1),
        column_names: texts(columns),
    })
}

#[tokio::test]
async fn select_distinct_keeps_the_first_of_each_key_and_refuses_unknown_columns() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let response = call(&mut c, distinct(2, &["S"])).await.unwrap();
    assert_eq!(response.size, 2);
    assert!(response.is_static);
    assert_eq!(column_names(c.table(2).as_ref()), ["S"]);
    assert_eq!(values(c.table(2).as_ref(), "S"), texts(&["a", "b"]));

    // Java checks the names itself and lists the missing ones in request order
    let refused = call(&mut c, distinct(3, &["Y", "S", "X=I"]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "column(s) not found: Y, X=I");
}

// ---------------------------------------------------------------- joins

/// Exports, as `id`, a table of one row per element of `rows`, each a list of formulas of
/// constants.
async fn table_of(c: &mut Connected, id: i32, rows: &[&[&str]]) {
    let mut ops = Vec::new();
    let mut sources = Vec::new();
    for (row, columns) in rows.iter().enumerate() {
        let row = i32::try_from(row).unwrap();
        ops.push(Op::EmptyTable(empty(None, 1)));
        ops.push(Op::Update(SelectOrUpdateRequest {
            result_id: None,
            source_id: by_offset(2 * row),
            column_specs: texts(columns),
        }));
        sources.push(by_offset(2 * row + 1).unwrap());
    }
    ops.push(Op::Merge(MergeTablesRequest {
        result_id: ticket(id),
        source_ids: sources,
        key_column: String::new(),
    }));
    let (responses, status) = c.batch(ops).await.unwrap();
    assert!(status.is_none(), "{status:?}");
    assert!(responses.iter().all(|response| response.success));
}

/// `S` = a, b, b and `W` = 1, 2, 3: key `a` once, key `b` twice.
async fn words(c: &mut Connected, id: i32) {
    table_of(
        c,
        id,
        &[
            &["S = `a`", "W = 1"],
            &["S = `b`", "W = 2"],
            &["S = `b`", "W = 3"],
        ],
    )
    .await;
}

fn natural(result: i32, right: i32, on: &[&str], add: &[&str], join_type: i32) -> Op {
    Op::NaturalJoin(proto::NaturalJoinTablesRequest {
        result_id: ticket(result),
        left_id: by_ticket(1),
        right_id: by_ticket(right),
        columns_to_match: texts(on),
        columns_to_add: texts(add),
        join_type,
    })
}

#[tokio::test]
async fn natural_join_adds_the_matching_right_row_by_join_type() {
    use proto::natural_join_tables_request::JoinType;
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    words(&mut c, 2).await;
    table_of(&mut c, 3, &[&["Key = `b`", "W = 7"]]).await;

    // match and addition spelled with space and `==`, and renamed
    let not_specified = JoinType::NotSpecified as i32;
    let response = call(
        &mut c,
        natural(10, 3, &[" S == Key "], &["Word=W"], not_specified),
    )
    .await
    .unwrap();
    assert_eq!(response.size, 6);
    let table = c.table(10);
    assert_eq!(column_names(table.as_ref()), ["I", "N", "S", "Word"]);
    assert_eq!(
        values(table.as_ref(), "Word"),
        texts(&["", "7", "", "7", "", "7"])
    );

    for (result, join_type, b) in [
        (11, JoinType::FirstMatch, "2"),
        (12, JoinType::LastMatch, "3"),
    ] {
        call(&mut c, natural(result, 2, &["S"], &["W"], join_type as i32))
            .await
            .unwrap();
        assert_eq!(
            values(c.table(result).as_ref(), "W"),
            texts(&["1", b, "1", b, "1", b])
        );
    }

    // a duplicate right key fails the join types that do not choose; so does a type the
    // protocol does not have, in Java an exception out of `adapt`
    for (result, join_type) in [
        (20, not_specified),
        (21, JoinType::ErrorOnDuplicate as i32),
        (22, JoinType::ExactlyOneMatch as i32),
        (23, 99),
    ] {
        let refused = call(&mut c, natural(result, 2, &["S"], &["W"], join_type))
            .await
            .unwrap_err();
        assert_internal(&refused);
    }
    // and so does every refusal of the engine: here a missing column and a type mismatch
    let first = JoinType::FirstMatch as i32;
    for (result, on, add) in [(24, "S=Nope", "W"), (25, "S=W", "Key")] {
        let refused = call(&mut c, natural(result, 3, &[on], &[add], first))
            .await
            .unwrap_err();
        assert_internal(&refused);
    }
}

#[tokio::test]
async fn exact_join_needs_one_right_row_for_every_left_row() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    table_of(&mut c, 2, &[&["S = `a`", "W = 1"], &["S = `b`", "W = 2"]]).await;
    table_of(&mut c, 3, &[&["S = `a`", "W = 1"]]).await;
    let exact = |result: i32, right: i32| {
        Op::ExactJoin(proto::ExactJoinTablesRequest {
            result_id: ticket(result),
            left_id: by_ticket(1),
            right_id: by_ticket(right),
            columns_to_match: texts(&["S"]),
            columns_to_add: Vec::new(),
        })
    };
    let response = call(&mut c, exact(10, 2)).await.unwrap();
    assert_eq!(response.size, 6);
    assert_eq!(
        values(c.table(10).as_ref(), "W"),
        texts(&["1", "2", "1", "2", "1", "2"])
    );
    assert_internal(&call(&mut c, exact(11, 3)).await.unwrap_err());
}

#[tokio::test]
async fn join_columns_are_read_first_and_left_join_is_unimplemented() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    words(&mut c, 2).await;
    let left = |result: i32, on: &[&str], add: &[&str]| {
        Op::LeftJoin(proto::LeftJoinTablesRequest {
            result_id: ticket(result),
            left_id: by_ticket(1),
            right_id: by_ticket(2),
            columns_to_match: texts(on),
            columns_to_add: texts(add),
        })
    };

    let refused = call(&mut c, left(10, &["S"], &["W"])).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "LeftJoinTables is currently unimplemented"
    );

    // the columns of every join are checked first, and refused with a status of their own
    let unparsable = [
        left(11, &["S"], &["W = 1"]),
        natural(12, 2, &["S", "A B"], &[], 2),
        Op::ExactJoin(proto::ExactJoinTablesRequest {
            result_id: ticket(13),
            left_id: by_ticket(1),
            right_id: by_ticket(2),
            columns_to_match: texts(&["S=in"]),
            columns_to_add: Vec::new(),
        }),
    ];
    let messages = [
        "Unable to parse expression: \"W = 1\": W = 1",
        "Unable to parse expression: \"A B\": A B",
        "Failed to get expression for all matched patterns: S=in",
    ];
    for (op, message) in unparsable.into_iter().zip(messages) {
        let refused = call(&mut c, op.clone()).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);

        // in a batch, before any operation of the batch is created
        let refused = c
            .batch(vec![Op::EmptyTable(empty(ticket(20), 1)), op])
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert_eq!(refused.message(), message);
        let unused = c.server.session(&c.bearer).get_export(20).unwrap();
        assert_ne!(unused.state(), ExportState::Exported);
    }
}

// ---------------------------------------------------------------- where in

fn where_in(result: i32, right: i32, on: &[&str], inverted: bool) -> proto::WhereInRequest {
    proto::WhereInRequest {
        result_id: ticket(result),
        left_id: by_ticket(1),
        right_id: by_ticket(right),
        inverted,
        columns_to_match: texts(on),
    }
}

#[tokio::test]
async fn where_in_keeps_the_rows_with_a_key_of_the_other_table_or_the_others() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    table_of(
        &mut c,
        2,
        &[
            &["Key = `b`", "W = 3"],
            &["Key = `zz`", "W = 4"],
            &["Key = `b`", "W = 9"],
        ],
    )
    .await;

    // a match spelled with space and `==`, and renamed
    let op = Op::WhereIn(where_in(10, 2, &[" S == Key "], false));
    let response = call(&mut c, op).await.unwrap();
    assert_eq!(response.size, 3);
    assert!(response.is_static);
    let table = c.table(10);
    assert_eq!(column_names(table.as_ref()), ["I", "N", "S"]);
    assert_eq!(values(table.as_ref(), "I"), ints(&[1, 3, 5]));

    let op = Op::WhereIn(where_in(11, 2, &["S=Key"], true));
    assert_eq!(call(&mut c, op).await.unwrap().size, 3);
    assert_eq!(values(c.table(11).as_ref(), "I"), ints(&[0, 2, 4]));

    // several columns are one key
    let both = ["S=Key", "I = W"];
    let op = Op::WhereIn(where_in(12, 2, &both, false));
    assert_eq!(call(&mut c, op).await.unwrap().size, 1);
    assert_eq!(values(c.table(12).as_ref(), "I"), ints(&[3]));
    let op = Op::WhereIn(where_in(13, 2, &both, true));
    assert_eq!(call(&mut c, op).await.unwrap().size, 5);
    assert_eq!(values(c.table(13).as_ref(), "I"), ints(&[0, 1, 2, 4, 5]));

    // what the engine refuses is an exception in Java: a column either table lacks, two
    // types; and so is a match that cannot be read, which Java reads in `create`
    for (result, on) in [
        (20, &["S=Nope"][..]),
        (21, &["Nope=Key"]),
        (22, &["S=W"]),
        (23, &["S="]),
        (24, &["S=="]),
        (25, &["=Key"]),
        (26, &["S Key"]),
        (27, &["S=Key", "1I=W"]),
        (28, &["S=Key=W"]),
    ] {
        for inverted in [false, true] {
            let result = result + 10 * i32::from(inverted);
            let op = Op::WhereIn(where_in(result, 2, on, inverted));
            assert_internal(&call(&mut c, op).await.unwrap_err());
        }
    }
}

#[tokio::test]
async fn where_in_requests_are_checked_as_java_checks_them() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let no_ref = Some(TableReference { r#ref: None });
    let empty_ticket = Some(ticket_reference(Vec::new()));
    let oneof = "io.deephaven.proto.backplane.grpc.TableReference must have oneof ref. Note: \
        this may also indicate that the server is older than the client and doesn't know about \
        this new oneof option.";
    let request = || where_in(2, 1, &["S"], false);
    let left = |left_id| proto::WhereInRequest {
        left_id,
        ..request()
    };
    let right = |right_id| proto::WhereInRequest {
        right_id,
        ..request()
    };
    let no_columns = "io.deephaven.proto.backplane.grpc.WhereInRequest must have at least one \
        columns_to_match (5)";
    let cases = [
        (
            left(None),
            "io.deephaven.proto.backplane.grpc.WhereInRequest must have field left_id (2)",
        ),
        (
            right(None),
            "io.deephaven.proto.backplane.grpc.WhereInRequest must have field right_id (3)",
        ),
        (where_in(2, 1, &[], false), no_columns),
        (where_in(2, 1, &[], true), no_columns),
        (left(no_ref.clone()), oneof),
        (right(empty_ticket.clone()), "Ticket is empty"),
        // in Java's order: the fields, then the columns, then the references
        (
            proto::WhereInRequest {
                right_id: None,
                ..where_in(2, 1, &[], false)
            },
            "io.deephaven.proto.backplane.grpc.WhereInRequest must have field right_id (3)",
        ),
        (
            proto::WhereInRequest {
                left_id: no_ref,
                ..where_in(2, 1, &[], false)
            },
            no_columns,
        ),
        (
            proto::WhereInRequest {
                left_id: empty_ticket,
                right_id: Some(TableReference { r#ref: None }),
                ..request()
            },
            "Ticket is empty",
        ),
    ];
    for (request, message) in cases {
        let refused = call(&mut c, Op::WhereIn(request.clone()))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);

        // in a batch, before any operation of the batch is created
        let refused = c
            .batch(vec![
                Op::EmptyTable(empty(ticket(20), 1)),
                Op::WhereIn(request),
            ])
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert_eq!(refused.message(), message);
        let unused = c.server.session(&c.bearer).get_export(20).unwrap();
        assert_ne!(unused.state(), ExportState::Exported);
    }
}

#[tokio::test]
async fn where_in_of_a_ticking_set_follows_it() {
    let mut c = Connected::start().await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    // the rows of the time table up to now, which stay as they are
    let snapshot = Op::Snapshot(proto::SnapshotTableRequest {
        result_id: ticket(2),
        source_id: by_ticket(1),
    });
    let fixed = call(&mut c, snapshot).await.unwrap().size;
    assert!(fixed > 0);

    // a static left table and a ticking set: the result refreshes and is not add-only
    let request = |result: i32, inverted: bool| proto::WhereInRequest {
        left_id: by_ticket(2),
        ..where_in(result, 1, &["Timestamp"], inverted)
    };
    let response = call(&mut c, Op::WhereIn(request(3, false))).await.unwrap();
    assert!(!response.is_static);
    assert_eq!(response.size, fixed);
    let attributes: Vec<(String, String)> = schema_metadata(&response)
        .into_iter()
        .filter(|(key, _)| key.starts_with("deephaven:attribute."))
        .collect();
    assert_eq!(attributes, pairs(&[]));
    let response = call(&mut c, Op::WhereIn(request(4, true))).await.unwrap();
    assert!(!response.is_static);
    assert_eq!(response.size, 0);

    // the other way round the result grows with the left table
    let request = proto::WhereInRequest {
        left_id: by_ticket(1),
        ..where_in(5, 2, &["Timestamp"], true)
    };
    let response = call(&mut c, Op::WhereIn(request)).await.unwrap();
    assert!(!response.is_static);
    let (ticking, late) = (c.table(1), c.table(5));
    let graph = c.server.state.graph.clone();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    let (all, others) = locked(graph.as_ref(), || {
        (ticking.row_set().size(), late.row_set().size())
    });
    assert!(all > fixed as u64);
    assert_eq!(others, all - fixed as u64);
}

// ---------------------------------------------------------------- snapshots

fn snapshot_of(result: i32, source: Option<TableReference>) -> Op {
    Op::Snapshot(proto::SnapshotTableRequest {
        result_id: ticket(result),
        source_id: source,
    })
}

/// A snapshot of table 1 when `trigger` changes, with a first snapshot at once.
fn when(result: i32, trigger: i32, stamps: &[&str]) -> proto::SnapshotWhenTableRequest {
    proto::SnapshotWhenTableRequest {
        result_id: ticket(result),
        base_id: by_ticket(1),
        trigger_id: by_ticket(trigger),
        initial: true,
        incremental: false,
        history: false,
        stamp_columns: texts(stamps),
    }
}

#[tokio::test]
async fn snapshot_copies_a_refreshing_table_into_a_static_one() {
    let mut c = Connected::start().await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    let response = call(&mut c, snapshot_of(2, by_ticket(1))).await.unwrap();
    assert!(response.is_static);
    assert!(response.size > 0);
    let copied = values(c.table(2).as_ref(), "Timestamp");
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    assert_eq!(values(c.table(2).as_ref(), "Timestamp"), copied);
}

#[tokio::test]
async fn snapshot_requests_are_checked_as_java_checks_them() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let no_ref = Some(TableReference { r#ref: None });
    let empty_ticket = Some(ticket_reference(Vec::new()));
    let oneof = "io.deephaven.proto.backplane.grpc.TableReference must have oneof ref. Note: \
        this may also indicate that the server is older than the client and doesn't know about \
        this new oneof option.";
    let base = |base_id| {
        Op::SnapshotWhen(proto::SnapshotWhenTableRequest {
            base_id,
            ..when(2, 1, &[])
        })
    };
    let trigger = |trigger_id| {
        Op::SnapshotWhen(proto::SnapshotWhenTableRequest {
            trigger_id,
            ..when(2, 1, &[])
        })
    };
    for (op, message) in [
        (
            snapshot_of(2, None),
            "io.deephaven.proto.backplane.grpc.SnapshotTableRequest must have field source_id (2)",
        ),
        (snapshot_of(2, no_ref.clone()), oneof),
        (snapshot_of(2, empty_ticket.clone()), "Ticket is empty"),
        (
            base(None),
            "io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest must have field base_id (2)",
        ),
        (
            trigger(None),
            "io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest must have field trigger_id (3)",
        ),
        (base(no_ref), oneof),
        (trigger(empty_ticket), "Ticket is empty"),
    ] {
        let refused = call(&mut c, op).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);
    }

    // history goes alone: Java's UnsupportedOperationException is UNIMPLEMENTED
    for (initial, incremental) in [(true, false), (false, true), (true, true)] {
        let request = proto::SnapshotWhenTableRequest {
            initial,
            incremental,
            history: true,
            ..when(3, 1, &[])
        };
        let refused = call(&mut c, Op::SnapshotWhen(request)).await.unwrap_err();
        assert_eq!(refused.code(), Code::Unimplemented);
        assert_eq!(
            refused.message(),
            "snapshotWhen with history does not currently support incremental nor initial. \
             See https://github.com/deephaven/deephaven-core/issues/3260."
        );
    }

    // a stamp column that names no column is an exception in Java, met before history is
    // checked
    for stamps in [&["1N=I"][..], &["int"], &["=I"], &["A=B=C"], &["I=", "N"]] {
        let request = proto::SnapshotWhenTableRequest {
            history: true,
            ..when(4, 1, stamps)
        };
        assert_internal(&call(&mut c, Op::SnapshotWhen(request)).await.unwrap_err());
    }
    // and so, in the engine, is a stamp column the trigger lacks or the base has
    for (result, stamps) in [(5, &["Nope"][..]), (6, &["S"])] {
        let request = when(result, 1, stamps);
        assert_internal(&call(&mut c, Op::SnapshotWhen(request)).await.unwrap_err());
    }
}

#[tokio::test]
async fn snapshot_when_stamps_the_copy_with_the_trigger() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    table_of(&mut c, 2, &[&["Tick = 7", "Other = 1"]]).await;

    // the stamp column names are trimmed as Java's `String#trim` trims, at and below the
    // space
    let request = when(3, 2, &["\u{1}Stamp = Tick\u{1f}"]);
    let response = call(&mut c, Op::SnapshotWhen(request)).await.unwrap();
    assert!(response.is_static);
    assert_eq!(response.size, 6);
    let table = c.table(3);
    assert_eq!(column_names(table.as_ref()), ["I", "N", "S", "Stamp"]);
    assert_eq!(values(table.as_ref(), "Stamp"), ints(&[7; 6]));

    // and not as Rust trims: a no-break space stays, and cannot be part of a name
    let request = when(4, 2, &["\u{a0}Tick"]);
    assert_internal(&call(&mut c, Op::SnapshotWhen(request)).await.unwrap_err());

    // without `initial` the copy waits for the trigger to change
    let request = proto::SnapshotWhenTableRequest {
        initial: false,
        ..when(5, 2, &[])
    };
    let response = call(&mut c, Op::SnapshotWhen(request)).await.unwrap();
    assert_eq!(response.size, 0);
    assert_eq!(
        column_names(c.table(5).as_ref()),
        ["I", "N", "S", "Tick", "Other"]
    );
}

#[tokio::test]
async fn snapshot_when_of_a_ticking_trigger_follows_it() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(2))))
        .await
        .unwrap();
    let request = proto::SnapshotWhenTableRequest {
        initial: false,
        ..when(3, 2, &[])
    };
    let response = call(&mut c, Op::SnapshotWhen(request)).await.unwrap();
    assert!(!response.is_static);
    assert_eq!(response.size, 0);
    let result = c.table(3);
    let graph = c.server.state.graph.clone();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    let size = locked(graph.as_ref(), || result.row_set().size());
    assert_eq!(size, 6);
}
