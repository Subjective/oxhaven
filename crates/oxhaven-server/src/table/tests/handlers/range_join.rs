//! The `RangeJoinTables` handler, called as a client calls it. Every status and message
//! asserted here is what Java 42.9 answered for the same request.

use super::*;
use oxhaven_contracts::schema::ColumnType;
use proto::agg_spec::{AggSpecGroup, AggSpecSum, AggSpecUnique, Type as SpecType};
use proto::aggregation::{AggregationColumns, AggregationCount, Type as AggregationType};
use proto::range_join_tables_request::{RangeEndRule, RangeStartRule};
use proto::{AggSpec, Aggregation, RangeJoinTablesRequest};

fn columns(spec: SpecType, pairs: &[&str]) -> Aggregation {
    Aggregation {
        r#type: Some(AggregationType::Columns(AggregationColumns {
            spec: Some(AggSpec {
                r#type: Some(spec),
            }),
            match_pairs: texts(pairs),
        })),
    }
}

fn group(pairs: &[&str]) -> Aggregation {
    columns(SpecType::Group(AggSpecGroup {}), pairs)
}

fn sum(pairs: &[&str]) -> Aggregation {
    columns(SpecType::Sum(AggSpecSum {}), pairs)
}

/// The range join of exports 1 and 2 on `K` and `Lo <= P <= Hi`, as text, grouping `V`.
fn by_text(result: i32) -> RangeJoinTablesRequest {
    RangeJoinTablesRequest {
        result_id: ticket(result),
        left_id: by_ticket(1),
        right_id: by_ticket(2),
        exact_match_columns: texts(&["K"]),
        range_match: "Lo <= P <= Hi".to_string(),
        aggregations: vec![group(&["Vs = V"])],
        ..Default::default()
    }
}

/// The same join with the range match named by its parts.
fn by_parts(result: i32) -> RangeJoinTablesRequest {
    RangeJoinTablesRequest {
        range_match: String::new(),
        left_start_column: "Lo".to_string(),
        range_start_rule: RangeStartRule::LessThanOrEqual as i32,
        right_range_column: "P".to_string(),
        range_end_rule: RangeEndRule::GreaterThanOrEqual as i32,
        left_end_column: "Hi".to_string(),
        ..by_text(result)
    }
}

/// Export 1: `Id`, `K`, `Lo`, `Hi`. Export 2: `K`, `P` (ascending in each `K`), `V`.
async fn tables(c: &mut Connected) {
    table_of(
        c,
        1,
        &[
            &["Id = 1", "K = `a`", "Lo = 1", "Hi = 5"],
            &["Id = 2", "K = `a`", "Lo = 4", "Hi = 4"],
            &["Id = 3", "K = `a`", "Lo = 6", "Hi = 2"],
            &["Id = 4", "K = `b`", "Lo = 0", "Hi = 9"],
            &["Id = 5", "K = `c`", "Lo = 0", "Hi = 9"],
        ],
    )
    .await;
    table_of(
        c,
        2,
        &[
            &["K = `a`", "P = 1", "V = 10"],
            &["K = `a`", "P = 3", "V = 30"],
            &["K = `b`", "P = 2", "V = 20"],
            &["K = `a`", "P = 5", "V = 50"],
        ],
    )
    .await;
}

async fn refused(c: &mut Connected, request: RangeJoinTablesRequest) -> Status {
    call(c, Op::RangeJoin(request)).await.unwrap_err()
}

/// The status of a request whose shape or matches Java refuses: `INVALID_ARGUMENT` with
/// `message`, from the one-shot call and from a batch, which is refused whole.
async fn assert_invalid(c: &mut Connected, request: RangeJoinTablesRequest, message: &str) {
    let status = refused(c, request.clone()).await;
    assert_eq!(status.code(), Code::InvalidArgument, "{message}");
    assert_eq!(status.message(), message);

    let unused = 900;
    let ops = vec![
        Op::EmptyTable(empty(ticket(unused), 1)),
        Op::RangeJoin(request),
    ];
    let status = c.batch(ops).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{message}");
    assert_eq!(status.message(), message);
    let session = c.server.session(&c.bearer);
    let state = session.get_export(unused).map(|export| export.state());
    assert_ne!(state.ok(), Some(ExportState::Exported), "{message}");
}

#[tokio::test]
async fn range_join_gives_each_left_row_a_vector_of_the_right_rows_in_its_range() {
    let mut c = Connected::start().await;
    tables(&mut c).await;

    let response = call(&mut c, Op::RangeJoin(by_text(10))).await.unwrap();
    assert!(response.success);
    assert!(response.is_static);
    assert_eq!(response.size, 5);
    // The result is a table of its own: none of the left table's attributes, not flat.
    assert_eq!(schema_metadata(&response), pairs(&[]));
    let schema = schema_of(&response);
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["Id", "K", "Lo", "Hi", "Vs"]);
    let vector = schema.field(4);
    assert_eq!(
        vector.metadata()["deephaven:type"],
        "io.deephaven.vector.IntVector"
    );
    assert_eq!(vector.metadata()["deephaven:componentType"], "int");
    assert_eq!(
        vector.data_type(),
        &ColumnType::vector(ColumnType::Int).arrow_type()
    );
    // An empty range is an empty vector; a row whose start is beyond its end has none.
    let expected = texts(&["[10, 30, 50]", "[]", "", "[20]", "[]"]);
    let table = c.table(10);
    assert_eq!(values(table.as_ref(), "Vs"), expected);
    assert_eq!(values(table.as_ref(), "Id"), ints(&[1, 2, 3, 4, 5]));

    // The same join by the parts of the match, with space around the names.
    let spaced = RangeJoinTablesRequest {
        left_start_column: " Lo\t".to_string(),
        exact_match_columns: texts(&[" K == K "]),
        ..by_parts(11)
    };
    let response = call(&mut c, Op::RangeJoin(spaced)).await.unwrap();
    assert_eq!(response.size, 5);
    assert_eq!(values(c.table(11).as_ref(), "Vs"), expected);

    // The parts can name what the text cannot: the row before with a strict end, and
    // the row after with a strict start. The rules are those of the request. A row whose
    // bounds are equal has a range only when both rules allow equality.
    let preceding = RangeJoinTablesRequest {
        range_start_rule: RangeStartRule::LessThanOrEqualAllowPreceding as i32,
        range_end_rule: RangeEndRule::GreaterThan as i32,
        ..by_parts(12)
    };
    call(&mut c, Op::RangeJoin(preceding)).await.unwrap();
    assert_eq!(
        values(c.table(12).as_ref(), "Vs"),
        texts(&["[10, 30]", "", "", "[20]", "[]"])
    );
    let strict = RangeJoinTablesRequest {
        range_start_rule: RangeStartRule::LessThan as i32,
        range_end_rule: RangeEndRule::GreaterThan as i32,
        ..by_parts(13)
    };
    call(&mut c, Op::RangeJoin(strict)).await.unwrap();
    assert_eq!(
        values(c.table(13).as_ref(), "Vs"),
        texts(&["[30]", "", "", "[20]", "[]"])
    );
    let following = RangeJoinTablesRequest {
        range_start_rule: RangeStartRule::LessThan as i32,
        range_end_rule: RangeEndRule::GreaterThanOrEqualAllowFollowing as i32,
        ..by_parts(14)
    };
    call(&mut c, Op::RangeJoin(following)).await.unwrap();
    assert_eq!(
        values(c.table(14).as_ref(), "Vs"),
        texts(&["[30, 50]", "", "", "[20]", "[]"])
    );
    // And the text names its own rules.
    let text = RangeJoinTablesRequest {
        range_match: "<- Lo <= P < Hi".to_string(),
        ..by_text(15)
    };
    call(&mut c, Op::RangeJoin(text)).await.unwrap();
    assert_eq!(
        values(c.table(15).as_ref(), "Vs"),
        texts(&["[10, 30]", "", "", "[20]", "[]"])
    );

    // Without exact matches every right row is in the one bucket, which is then out of
    // order: an exception out of the engine.
    let unkeyed = RangeJoinTablesRequest {
        exact_match_columns: Vec::new(),
        ..by_text(16)
    };
    assert_internal(&refused(&mut c, unkeyed).await);

    // Two aggregations, several pairs, and an output that takes a left column's place.
    let several = RangeJoinTablesRequest {
        aggregations: vec![group(&["Lo = V", " Ps = P "]), group(&["K"])],
        ..by_text(17)
    };
    let response = call(&mut c, Op::RangeJoin(several)).await.unwrap();
    let schema = schema_of(&response);
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["Id", "K", "Lo", "Hi", "Ps"]);
    assert_eq!(
        schema.field(1).metadata()["deephaven:type"],
        "io.deephaven.vector.ObjectVector"
    );
    assert_eq!(
        schema.field(1).metadata()["deephaven:componentType"],
        "java.lang.String"
    );
    assert_eq!(
        values(c.table(17).as_ref(), "K"),
        texts(&["[a, a, a]", "[]", "", "[b]", "[]"])
    );
    assert_eq!(
        values(c.table(17).as_ref(), "Lo"),
        texts(&["[10, 30, 50]", "[]", "", "[20]", "[]"])
    );
}

#[tokio::test]
async fn range_join_runs_in_a_batch_on_tables_of_the_batch_and_on_its_own_result() {
    let mut c = Connected::start().await;
    tables(&mut c).await;
    let first = RangeJoinTablesRequest {
        result_id: None,
        left_id: by_offset(0),
        right_id: by_offset(1),
        ..by_text(0)
    };
    // The result of a range join is the left table of another, which carries its vector
    // column, and the right table of a third, which groups it.
    let carried = RangeJoinTablesRequest {
        left_id: by_offset(2),
        aggregations: vec![group(&["Ps = P"])],
        ..by_text(20)
    };
    let nested = RangeJoinTablesRequest {
        right_id: by_offset(2),
        range_match: "Lo <= Lo <= Hi".to_string(),
        aggregations: vec![group(&["Vss = Vs"])],
        ..by_text(21)
    };
    let flatten = |id: i32| {
        Op::Flatten(FlattenRequest {
            result_id: None,
            source_id: by_ticket(id),
        })
    };
    let ops = vec![
        flatten(1),
        flatten(2),
        Op::RangeJoin(first),
        Op::RangeJoin(carried),
        Op::RangeJoin(nested),
    ];
    let (responses, status) = c.batch(ops).await.unwrap();
    assert!(status.is_none(), "{status:?}");
    assert_eq!(responses.len(), 5);
    assert!(responses.iter().all(|response| response.success));

    let table = c.table(20);
    assert_eq!(
        column_names(table.as_ref()),
        ["Id", "K", "Lo", "Hi", "Vs", "Ps"]
    );
    assert_eq!(
        values(table.as_ref(), "Vs"),
        texts(&["[10, 30, 50]", "[]", "", "[20]", "[]"])
    );
    assert_eq!(
        values(table.as_ref(), "Ps"),
        texts(&["[1, 3, 5]", "[]", "", "[2]", "[]"])
    );

    // Bucket a has the left rows with Lo 1, 4 and 6, of which the last has no vector.
    let table = c.table(21);
    let nested = table.definition().index_of("Vss").unwrap();
    assert_eq!(
        table.definition().columns()[nested].column_type,
        ColumnType::vector(ColumnType::vector(ColumnType::Int))
    );
    assert_eq!(
        values(table.as_ref(), "Vss"),
        texts(&["[[10, 30, 50], []]", "[[]]", "", "[[20]]", "[[]]"])
    );
    let described = responses
        .iter()
        .find(|response| response.result_id == by_ticket(21))
        .unwrap();
    let field = schema_of(described).field(4).clone();
    assert_eq!(
        field.metadata()["deephaven:type"],
        "io.deephaven.vector.ObjectVector"
    );
    assert_eq!(
        field.metadata()["deephaven:componentType"],
        "io.deephaven.vector.IntVector"
    );
}

const PREFIX: &str = "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest";
const ONE_OF: &str = "Note: this may also indicate that the server is older than the client and \
                      doesn't know about this new oneof option.";

#[tokio::test]
async fn the_shape_of_a_range_join_request_is_checked_in_java_s_order() {
    let mut c = Connected::start().await;
    tables(&mut c).await;
    let has = |field: &str| format!("{PREFIX} must have field {field}");
    let has_not = |field: &str| {
        format!(
            "If `range_match` is provided, range details should remain empty. \nInternal \
             error: INVALID_ARGUMENT: {PREFIX} must not have field {field}"
        )
    };
    let no_aggregations = format!("{PREFIX} must have at least one aggregations (10)");
    let no_ref = format!(
        "io.deephaven.proto.backplane.grpc.TableReference must have oneof ref. {ONE_OF}"
    );
    let empty_ticket = Some(TableReference {
        r#ref: Some(Ref::Ticket(Ticket { ticket: Vec::new() })),
    });
    let no_type = Aggregation { r#type: None };
    type Change = Box<dyn Fn(&mut RangeJoinTablesRequest)>;
    let change = |f: fn(&mut RangeJoinTablesRequest)| -> Change { Box::new(f) };
    // Each case has the faults of those after it as well, wherever the request allows,
    // so that the order of the checks shows.
    let text_cases: Vec<(Change, String)> = vec![
        (
            change(|r| {
                r.left_id = None;
                r.right_id = None;
                r.left_start_column = "Lo".into();
                r.aggregations.clear();
            }),
            has("left_id (2)"),
        ),
        (
            change(|r| {
                r.right_id = None;
                r.left_start_column = "Lo".into();
                r.aggregations.clear();
            }),
            has("right_id (3)"),
        ),
        (
            change(|r| {
                r.left_start_column = "Lo".into();
                r.range_start_rule = 1;
                r.right_range_column = "P".into();
                r.range_end_rule = 1;
                r.left_end_column = "Hi".into();
                r.aggregations.clear();
            }),
            has_not("left_start_column (5)"),
        ),
        (
            change(|r| {
                r.range_start_rule = 99;
                r.right_range_column = "P".into();
                r.range_end_rule = 1;
                r.left_end_column = "Hi".into();
            }),
            has_not("range_start_rule (6)"),
        ),
        (
            change(|r| {
                r.right_range_column = "P".into();
                r.range_end_rule = 1;
                r.left_end_column = "Hi".into();
            }),
            has_not("right_range_column (7)"),
        ),
        (
            change(|r| {
                r.range_end_rule = 1;
                r.left_end_column = "Hi".into();
            }),
            has_not("range_end_rule (8)"),
        ),
        (
            change(|r| {
                r.left_end_column = "Hi".into();
                r.aggregations.clear();
            }),
            has_not("left_end_column (9)"),
        ),
        (
            change(|r| {
                r.aggregations.clear();
                r.left_id = Some(TableReference::default());
                r.range_match = "nope".into();
            }),
            no_aggregations.clone(),
        ),
        (
            change(|r| {
                r.left_id = Some(TableReference::default());
                r.right_id = Some(TableReference::default());
                r.exact_match_columns = vec!["K=".into()];
            }),
            no_ref.clone(),
        ),
        (
            change(|r| {
                r.right_id = Some(TableReference::default());
                r.exact_match_columns = vec!["K=".into()];
            }),
            no_ref.clone(),
        ),
    ];
    for (change, message) in &text_cases {
        let mut request = by_text(10);
        change(&mut request);
        assert_invalid(&mut c, request, message).await;
    }
    for (left, right) in [(empty_ticket.clone(), by_ticket(2)), (by_ticket(1), empty_ticket)] {
        let request = RangeJoinTablesRequest {
            left_id: left,
            right_id: right,
            exact_match_columns: texts(&["K="]),
            ..by_text(10)
        };
        assert_invalid(&mut c, request, "Ticket is empty").await;
    }

    // Without the text, the five parts are asked for in the order of their numbers, and
    // before the aggregations.
    let parts_cases: Vec<(Change, String)> = vec![
        (
            change(|r| {
                r.left_start_column.clear();
                r.range_start_rule = 0;
                r.right_range_column.clear();
                r.range_end_rule = 0;
                r.left_end_column.clear();
                r.aggregations.clear();
            }),
            has("left_start_column (5)"),
        ),
        (
            change(|r| {
                r.range_start_rule = 0;
                r.right_range_column.clear();
                r.aggregations.clear();
            }),
            has("range_start_rule (6)"),
        ),
        (
            change(|r| {
                r.right_range_column.clear();
                r.range_end_rule = 0;
            }),
            has("right_range_column (7)"),
        ),
        (
            change(|r| {
                r.range_end_rule = 0;
                r.left_end_column.clear();
            }),
            has("range_end_rule (8)"),
        ),
        (
            change(|r| {
                r.left_end_column.clear();
                r.aggregations.clear();
            }),
            has("left_end_column (9)"),
        ),
        (change(|r| r.aggregations.clear()), no_aggregations),
    ];
    for (change, message) in &parts_cases {
        let mut request = by_parts(10);
        change(&mut request);
        assert_invalid(&mut c, request, message).await;
    }

    // The aggregations are looked at last, each as `Aggregate` looks at its own.
    let aggregation_cases = [
        (
            vec![group(&["Vs = V"]), no_type.clone()],
            format!("io.deephaven.proto.backplane.grpc.Aggregation must have oneof type. {ONE_OF}"),
        ),
        (
            vec![Aggregation {
                r#type: Some(AggregationType::Columns(AggregationColumns {
                    spec: None,
                    match_pairs: texts(&["V"]),
                })),
            }],
            "io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns must have field \
             spec (1)"
                .to_string(),
        ),
        (
            vec![group(&[])],
            "io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns must have at \
             least one match_pairs (2)"
                .to_string(),
        ),
        (
            vec![Aggregation {
                r#type: Some(AggregationType::Columns(AggregationColumns {
                    spec: Some(AggSpec { r#type: None }),
                    match_pairs: texts(&["V"]),
                })),
            }],
            format!("io.deephaven.proto.backplane.grpc.AggSpec must have oneof type. {ONE_OF}"),
        ),
        (
            vec![columns(SpecType::Unique(AggSpecUnique::default()), &["V"])],
            "io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique must have field \
             non_unique_sentinel (2)"
                .to_string(),
        ),
    ];
    for (aggregations, message) in aggregation_cases {
        let request = RangeJoinTablesRequest {
            aggregations,
            ..by_text(10)
        };
        assert_invalid(&mut c, request, &message).await;
    }
    // A match that cannot be read is refused before a malformed aggregation is.
    for (exact, range, message) in [
        (
            "K=",
            "nope",
            "Unable to parse match 'K=', expected form '<left>==<right>' or `<left>=<right>`",
        ),
        (
            "K",
            "Lo <= P <= Lo",
            "RangeJoinMatch must have distinct ColumnNames for leftStartColumn() and \
             leftEndColumn() in order to properly define a range, Lo specified for both fields",
        ),
    ] {
        let request = RangeJoinTablesRequest {
            exact_match_columns: texts(&[exact]),
            range_match: range.to_string(),
            aggregations: vec![no_type.clone()],
            ..by_text(10)
        };
        assert_invalid(&mut c, request, message).await;
    }

    // None of these consumed the result ticket.
    let response = call(&mut c, Op::RangeJoin(by_text(10))).await.unwrap();
    assert_eq!(response.size, 5);
}

#[tokio::test]
async fn the_matches_of_a_range_join_request_are_read_as_java_reads_them() {
    let mut c = Connected::start().await;
    tables(&mut c).await;
    let pattern = "\\A\\s*(<-)?\\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\\s*(<=?)\\s*\
                   ([a-zA-Z_$][a-zA-Z0-9_$]*)\\s*(<=?)\\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\\s*(->)?\\s*\\Z";
    let unfit = |text: &str| format!("Input expression {text} does not match expected pattern {pattern}");
    let reserved =
        |name: &str| format!("Invalid column name \"{name}\": \"{name}\" is a reserved keyword");
    let same = "RangeJoinMatch must have distinct ColumnNames for leftStartColumn() and \
                leftEndColumn() in order to properly define a range, Lo specified for both fields";
    let preceding = |text: &str| {
        format!(
            "Error parsing expression {text}: <- operator for \"allow preceding\" may only be \
             combined with <= start match expression"
        )
    };
    let following = |text: &str| {
        format!(
            "Error parsing expression {text}: -> operator for \"allow following\" may only be \
             combined with <= end match expression"
        )
    };

    // The exact matches, in order, the first fault only.
    for (exact, message) in [
        (
            vec!["K", "K==", "in"],
            "Unable to parse match 'K==', expected form '<left>==<right>' or `<left>=<right>`"
                .to_string(),
        ),
        (vec!["K=in", "K="], reserved("in")),
        (vec!["A=B=C"], "Invalid column name \"B=C\"".to_string()),
        (
            vec![""],
            "Invalid column name: name can not be null or empty".to_string(),
        ),
    ] {
        let request = RangeJoinTablesRequest {
            exact_match_columns: texts(&exact),
            range_match: "nope".to_string(),
            ..by_text(10)
        };
        assert_invalid(&mut c, request, &message).await;
        // The exact matches are read the same way when the range match is given by parts.
        let request = RangeJoinTablesRequest {
            exact_match_columns: texts(&exact),
            left_start_column: "in".to_string(),
            ..by_parts(10)
        };
        assert_invalid(&mut c, request, &message).await;
    }

    // The range match as text: the names are checked before the rules and the bounds.
    for (text, message) in [
        ("Lo <= P", unfit("Lo <= P")),
        ("Lo <== P <= Hi", unfit("Lo <== P <= Hi")),
        ("Lo <= P <= Hi\u{2028}", unfit("Lo <= P <= Hi\u{2028}")),
        ("<- Lo < P <= Hi", preceding("<- Lo < P <= Hi")),
        ("<- Lo < P < Hi ->", preceding("<- Lo < P < Hi ->")),
        ("Lo <= P < Hi ->", following("Lo <= P < Hi ->")),
        ("Lo <= P <= Lo", same.to_string()),
        ("<- Lo <= P <= Lo", same.to_string()),
        ("in <= P <= in", reserved("in")),
        ("<- in < P <= Hi", reserved("in")),
        ("Lo <= i <= k", reserved("i")),
        ("Lo <= P < k ->", reserved("k")),
    ] {
        let request = RangeJoinTablesRequest {
            range_match: text.to_string(),
            ..by_text(10)
        };
        assert_invalid(&mut c, request, &message).await;
    }

    // The range match by its parts, in the order start column, start rule, range
    // column, end rule, end column, and then the bounds.
    let start_rule = "Unrecognized range start rule UNRECOGNIZED for range join";
    let end_rule = "Unrecognized range end rule UNRECOGNIZED for range join";
    type Change = fn(&mut RangeJoinTablesRequest);
    let cases: [(Change, String); 7] = [
        (
            |r| {
                r.left_start_column = "1x".into();
                r.range_start_rule = 99;
            },
            "Invalid column name \"1x\"".to_string(),
        ),
        (
            |r| {
                r.range_start_rule = 99;
                r.right_range_column = "a b".into();
            },
            start_rule.to_string(),
        ),
        (
            |r| {
                r.right_range_column = "a b".into();
                r.range_end_rule = 99;
            },
            "Invalid column name \"a b\"".to_string(),
        ),
        (
            |r| {
                r.range_end_rule = 99;
                r.left_end_column = "in".into();
            },
            end_rule.to_string(),
        ),
        (
            |r| {
                r.left_end_column = "in".into();
                r.left_start_column = "in".into();
            },
            reserved("in"),
        ),
        (|r| r.left_end_column = "in".into(), reserved("in")),
        (|r| r.left_end_column = " Lo ".into(), same.to_string()),
    ];
    for (change, message) in cases {
        let mut request = by_parts(10);
        change(&mut request);
        assert_invalid(&mut c, request, &message).await;
    }

    // What Java reads: space around the text and its parts, and no space at all.
    for (result, text) in [
        (10, "\tLo<=P<=Hi\r\n"),
        (11, " <-Lo<=P<=Hi-> "),
        (12, "Lo < P < Hi"),
    ] {
        let request = RangeJoinTablesRequest {
            range_match: text.to_string(),
            ..by_text(result)
        };
        let response = call(&mut c, Op::RangeJoin(request)).await.unwrap();
        assert_eq!(response.size, 5, "{text:?}");
    }
}

#[tokio::test]
async fn what_the_range_join_itself_refuses_is_an_internal_error() {
    let mut c = Connected::start().await;
    tables(&mut c).await;
    c.client
        .time_table(c.auth(ticking_time_table(ticket(3))))
        .await
        .unwrap();

    let count = Aggregation {
        r#type: Some(AggregationType::Count(AggregationCount {
            column_name: "N".to_string(),
        })),
    };
    type Change = Box<dyn Fn(&mut RangeJoinTablesRequest)>;
    let with = |aggregations: Vec<Aggregation>| -> Change {
        Box::new(move |r| r.aggregations = aggregations.clone())
    };
    let on = |exact: &'static str, range: &'static str| -> Change {
        Box::new(move |r| {
            r.exact_match_columns = texts(&[exact]);
            r.range_match = range.to_string();
        })
    };
    let cases: Vec<(&str, Change)> = vec![
        ("sum", with(vec![sum(&["V"])])),
        ("group and sum", with(vec![group(&["V"]), sum(&["V"])])),
        ("count", with(vec![count])),
        ("a pair that is no pair", with(vec![group(&["X=Y=Z"])])),
        ("a pair of a reserved name", with(vec![group(&["in = V"])])),
        ("an input that is no column", with(vec![group(&["Nope"])])),
        (
            "an input of the left table",
            with(vec![group(&["Ids = Id"])]),
        ),
        ("no left exact column", on("Nope = K", "Lo <= P <= Hi")),
        ("no right exact column", on("K = Nope", "Lo <= P <= Hi")),
        ("exact columns of two types", on("Id = K", "Lo <= P <= Hi")),
        ("no range column", on("K", "Lo <= Nope <= Hi")),
        ("no bound column", on("K", "Nope <= P <= Hi")),
        ("a bound of another type", on("K", "K <= P <= Hi")),
        (
            "a refreshing left table",
            Box::new(|r| r.left_id = by_ticket(3)),
        ),
        (
            "a refreshing right table",
            Box::new(|r| r.right_id = by_ticket(3)),
        ),
    ];
    for (index, (what, change)) in cases.iter().enumerate() {
        let result = 10 + i32::try_from(index).unwrap();
        let mut request = by_text(result);
        change(&mut request);
        let status = refused(&mut c, request.clone()).await;
        assert_eq!(status.code(), Code::InvalidArgument, "{what}");
        assert!(
            status.message().starts_with("Details Logged w/ID '"),
            "{what}: {status:?}"
        );
        // The request was let through to the export, which failed: the ticket is used.
        let export = c.server.session(&c.bearer).get_export(result).unwrap();
        assert_eq!(export.state(), ExportState::Failed, "{what}");

        // In a batch the operation fails by the same error, which also ends the stream.
        request.result_id = ticket(100 + result);
        let ops = vec![
            Op::EmptyTable(empty(ticket(200 + result), 1)),
            Op::RangeJoin(request),
        ];
        let (responses, status) = c.batch(ops).await.unwrap();
        assert_internal(&status.unwrap_or_else(|| panic!("{what}: no status")));
        let failed = responses
            .iter()
            .find(|response| response.result_id == by_ticket(100 + result))
            .unwrap();
        assert!(!failed.success, "{what}");
        let (error_id, cause) = failed.error_info.split_once(' ').unwrap();
        assert_eq!(
            cause,
            format!("cause: INVALID_ARGUMENT: Details Logged w/ID '{error_id}'"),
            "{what}"
        );
        assert_eq!(c.table(200 + result).row_set().size(), 1, "{what}");
    }

    // Right rows out of order in a bucket a left row has.
    table_of(
        &mut c,
        4,
        &[
            &["K = `a`", "P = 5", "V = 1"],
            &["K = `a`", "P = 1", "V = 2"],
        ],
    )
    .await;
    let unsorted = RangeJoinTablesRequest {
        right_id: by_ticket(4),
        ..by_text(90)
    };
    assert_internal(&refused(&mut c, unsorted).await);
}
