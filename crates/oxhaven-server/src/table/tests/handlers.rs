//! The operator handlers, called as a client calls them.

use super::*;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::SortDescriptor;

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
