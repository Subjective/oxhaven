//! `Aj`, `Raj` and the deprecated `AsOfJoin`, called as a client calls them. Every status
//! and message here is what the Java server (Deephaven 42.9) answered for the same request.

use super::*;
use proto::as_of_join_tables_request::MatchRule;

/// The message of Java for an as-of match that is not `Left >= Right`.
fn no_match(text: &str) -> String {
    format!(
        "Input expression {text} does not match expected pattern \
         \\A\\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\\s*(>=?|<=?)\\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\\s*\\Z"
    )
}

/// Table 1, the left: `K` = a, b, a; `T` = 10, 20, 30; `L` = 0, 1, 2.
/// Table 2, the right: `K` = a, b, a, b; `T2` = 10, 15, 25, 40; `V` = 1, 2, 3, 4.
/// Table 3, a right with the left's stamp name: `T` = 10, 15, 25, 40; `W` = 1, 2, 3, 4.
async fn stamped(c: &mut Connected) {
    table_of(
        c,
        1,
        &[
            &["K = `a`", "T = 10", "L = 0"],
            &["K = `b`", "T = 20", "L = 1"],
            &["K = `a`", "T = 30", "L = 2"],
        ],
    )
    .await;
    table_of(
        c,
        2,
        &[
            &["K = `a`", "T2 = 10", "V = 1"],
            &["K = `b`", "T2 = 15", "V = 2"],
            &["K = `a`", "T2 = 25", "V = 3"],
            &["K = `b`", "T2 = 40", "V = 4"],
        ],
    )
    .await;
    table_of(
        c,
        3,
        &[
            &["T = 10", "W = 1"],
            &["T = 15", "W = 2"],
            &["T = 25", "W = 3"],
            &["T = 40", "W = 4"],
        ],
    )
    .await;
}

fn request(
    result: i32,
    right: i32,
    exact: &[&str],
    as_of: &str,
    add: &[&str],
) -> proto::AjRajTablesRequest {
    proto::AjRajTablesRequest {
        result_id: ticket(result),
        left_id: by_ticket(1),
        right_id: by_ticket(right),
        exact_match_columns: texts(exact),
        as_of_column: as_of.to_string(),
        columns_to_add: texts(add),
    }
}

fn legacy(result: i32, on: &[&str], add: &[&str], rule: i32) -> Op {
    Op::AsOfJoin(proto::AsOfJoinTablesRequest {
        result_id: ticket(result),
        left_id: by_ticket(1),
        right_id: by_ticket(2),
        columns_to_match: texts(on),
        columns_to_add: texts(add),
        as_of_match_rule: rule,
    })
}

async fn refusal(c: &mut Connected, op: Op) -> Status {
    call(c, op).await.unwrap_err()
}

#[tokio::test]
async fn aj_and_raj_add_the_closest_right_row_on_their_side() {
    let mut c = Connected::start().await;
    stamped(&mut c).await;
    type Wrap = fn(proto::AjRajTablesRequest) -> Op;
    let (aj, raj): (Wrap, Wrap) = (Op::Aj, Op::Raj);

    let cases: [(Wrap, &str, [&str; 3]); 6] = [
        (aj, "T>=T2", ["1", "2", "3"]),
        (aj, " T\t> T2\n", ["", "2", "3"]),
        (raj, "T<=T2", ["1", "3", "4"]),
        (raj, "T < T2", ["2", "3", "4"]),
        // the operator's rule the other way round is not the operation's default
        (aj, "T>=T2\r\n", ["1", "2", "3"]),
        (raj, "T\u{0B}<=\u{0C}T2", ["1", "3", "4"]),
    ];
    let mut next = 10;
    for (wrap, as_of, expected) in cases {
        next += 1;
        let response = call(&mut c, wrap(request(next, 2, &[], as_of, &["V"])))
            .await
            .unwrap();
        assert!(response.success);
        assert!(response.is_static);
        assert_eq!(response.size, 3);
        let table = c.table(next);
        assert_eq!(
            column_names(table.as_ref()),
            ["K", "T", "L", "T2", "V"],
            "{as_of:?}"
        );
        assert_eq!(values(table.as_ref(), "V"), texts(&expected), "{as_of:?}");
    }

    // a column name alone is that column on both sides, under `>=` for aj and `<=` for
    // raj; it is trimmed as `String#trim` trims
    for (result, wrap, as_of, expected) in [
        (20, aj, "T", ["1", "2", "3"]),
        (21, raj, "T", ["1", "3", "4"]),
        (22, aj, "\u{1} T\u{2}", ["1", "2", "3"]),
    ] {
        call(&mut c, wrap(request(result, 3, &[], as_of, &["W"])))
            .await
            .unwrap();
        let table = c.table(result);
        assert_eq!(column_names(table.as_ref()), ["K", "T", "L", "W"]);
        assert_eq!(values(table.as_ref(), "W"), texts(&expected), "{as_of:?}");
    }

    // exact matches, spelled as `JoinMatch#parse` reads them, and a renamed addition
    for (result, exact) in [(30, "K"), (31, " K == K "), (32, "\tK=K")] {
        let op = Op::Raj(request(result, 2, &[exact], "T<=T2", &["Value = V"]));
        call(&mut c, op).await.unwrap();
        let table = c.table(result);
        assert_eq!(
            column_names(table.as_ref()),
            ["K", "T", "L", "T2", "Value"],
            "{exact:?}"
        );
        assert_eq!(values(table.as_ref(), "Value"), texts(&["1", "4", ""]));
    }

    // with no additions, every right column that is not the left column of a match
    call(&mut c, Op::Aj(request(40, 2, &["K"], "T>=T2", &[])))
        .await
        .unwrap();
    assert_eq!(
        column_names(c.table(40).as_ref()),
        ["K", "T", "L", "T2", "V"]
    );
}

#[tokio::test]
async fn aj_and_raj_run_in_a_batch_on_tables_of_the_batch() {
    let mut c = Connected::start().await;
    stamped(&mut c).await;
    let by_offset_of = |right: i32, as_of: &str, add: &str| proto::AjRajTablesRequest {
        result_id: None,
        left_id: by_offset(0),
        right_id: by_offset(right),
        exact_match_columns: Vec::new(),
        as_of_column: as_of.to_string(),
        columns_to_add: texts(&[add]),
    };
    let ops = vec![
        fetch(by_ticket(1), None),
        fetch(by_ticket(2), None),
        Op::Aj(by_offset_of(1, "T>T2", "V")),
        Op::Raj(by_offset_of(1, "T<T2", "V")),
        Op::Aj(proto::AjRajTablesRequest {
            result_id: ticket(50),
            ..by_offset_of(3, "L >= V", "Other = K")
        }),
        legacy(51, &["T=T2"], &["V"], MatchRule::GreaterThan as i32),
    ];
    let (responses, end) = c.batch(ops).await.unwrap();
    assert!(end.is_none(), "{end:?}");
    assert_eq!(responses.len(), 6);
    assert!(responses.iter().all(|response| response.success));
    // all but the fetch of the right table, which has four rows
    let of_three = responses.iter().filter(|response| response.size == 3);
    assert_eq!(of_three.count(), 5);
    // the left's `L` = 0, 1, 2 against the `V` = 2, 3, 4 of the raj: only the last `L` has
    // a `V` that is not greater
    let table = c.table(50);
    assert_eq!(column_names(table.as_ref()), ["K", "T", "L", "V", "Other"]);
    assert_eq!(values(table.as_ref(), "V"), texts(&["", "", "2"]));
    assert_eq!(values(table.as_ref(), "Other"), texts(&["", "", "a"]));
    assert_eq!(values(c.table(51).as_ref(), "V"), texts(&["2", "3", "4"]));
}

#[tokio::test]
async fn aj_and_raj_refuse_the_rules_of_the_other_and_what_is_not_a_match() {
    let mut c = Connected::start().await;
    stamped(&mut c).await;
    let cases = [
        (
            Op::Aj(request(10, 2, &[], "T<=T2", &["V"])),
            "Unsupported operation for aj: LESS_THAN_EQUAL".to_string(),
        ),
        (
            Op::Aj(request(11, 2, &[], "T < T2", &["V"])),
            "Unsupported operation for aj: LESS_THAN".to_string(),
        ),
        (
            Op::Raj(request(12, 2, &[], "T>=T2", &["V"])),
            "Unsupported operation for raj: GREATER_THAN_EQUAL".to_string(),
        ),
        (
            Op::Raj(request(13, 2, &[], "T>T2", &["V"])),
            "Unsupported operation for raj: GREATER_THAN".to_string(),
        ),
        // the rule is read before the tables: these columns do not exist
        (
            Op::Raj(request(14, 2, &[], "Nope>=Nada", &["V"])),
            "Unsupported operation for raj: GREATER_THAN_EQUAL".to_string(),
        ),
        (
            Op::Aj(request(15, 2, &[], "T=T2", &["V"])),
            no_match("T=T2"),
        ),
        (
            Op::Raj(request(16, 2, &[], "T T2", &["V"])),
            no_match("T T2"),
        ),
        (Op::Aj(request(17, 2, &[], "in", &["V"])), no_match("in")),
        (Op::Aj(request(18, 2, &[], " ", &["V"])), no_match(" ")),
        (
            Op::Aj(request(19, 2, &[], "T>=T2\u{2028}", &["V"])),
            no_match("T>=T2\u{2028}"),
        ),
        (
            Op::Raj(request(20, 2, &[], "in<=T2", &["V"])),
            "Invalid column name \"in\": \"in\" is a reserved keyword".to_string(),
        ),
    ];
    for (op, message) in cases {
        let refused = refusal(&mut c, op.clone()).await;
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);

        // in a batch, before any operation of the batch is created
        let refused = c
            .batch(vec![Op::EmptyTable(empty(ticket(90), 1)), op])
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert_eq!(refused.message(), message);
        let unused = c.server.session(&c.bearer).get_export(90).unwrap();
        assert_ne!(unused.state(), ExportState::Exported);
    }
}

#[tokio::test]
async fn aj_checks_the_request_in_javas_order() {
    let mut c = Connected::start().await;
    stamped(&mut c).await;
    let field = |name: &str, number: u32| {
        format!(
            "io.deephaven.proto.backplane.grpc.AjRajTablesRequest must have field {name} ({number})"
        )
    };
    let no_ref = "io.deephaven.proto.backplane.grpc.TableReference must have oneof ref. Note: \
                  this may also indicate that the server is older than the client and doesn't \
                  know about this new oneof option.";
    let unset = Some(TableReference { r#ref: None });
    let empty_ticket = Some(TableReference {
        r#ref: Some(Ref::Ticket(Ticket { ticket: Vec::new() })),
    });
    // everything wrong at once, then one thing less each time
    let all_wrong = proto::AjRajTablesRequest {
        result_id: ticket(10),
        left_id: None,
        right_id: None,
        exact_match_columns: texts(&["K K"]),
        as_of_column: String::new(),
        columns_to_add: texts(&["V V"]),
    };
    let steps: Vec<(proto::AjRajTablesRequest, String)> = vec![
        (all_wrong.clone(), field("left_id", 2)),
        (
            proto::AjRajTablesRequest {
                left_id: unset.clone(),
                ..all_wrong.clone()
            },
            field("right_id", 3),
        ),
        (
            proto::AjRajTablesRequest {
                left_id: unset.clone(),
                right_id: empty_ticket.clone(),
                ..all_wrong.clone()
            },
            field("as_of_column", 5),
        ),
        (
            proto::AjRajTablesRequest {
                left_id: unset.clone(),
                right_id: empty_ticket.clone(),
                as_of_column: "T T2".to_string(),
                ..all_wrong.clone()
            },
            no_ref.to_string(),
        ),
        (
            proto::AjRajTablesRequest {
                left_id: by_ticket(1),
                right_id: empty_ticket,
                as_of_column: "T T2".to_string(),
                ..all_wrong.clone()
            },
            "Ticket is empty".to_string(),
        ),
        // the columns: the exact matches, the as-of match, the additions
        (
            proto::AjRajTablesRequest {
                exact_match_columns: texts(&["K", "K=", "K K"]),
                ..request(10, 2, &[], "T T2", &["V V"])
            },
            "Unable to parse match 'K=', expected form '<left>==<right>' or `<left>=<right>`"
                .to_string(),
        ),
        (
            request(10, 2, &["K", "K K"], "T T2", &["V V"]),
            "Invalid column name \"K K\"".to_string(),
        ),
        (request(10, 2, &["K"], "T T2", &["V V"]), no_match("T T2")),
        (
            request(10, 2, &["K"], "T<T2", &["V V"]),
            "Unsupported operation for aj: LESS_THAN".to_string(),
        ),
        (
            request(10, 2, &["K"], "T>=T2", &["V", "X==V", "V V"]),
            "Invalid column name \"=V\"".to_string(),
        ),
        (
            request(10, 2, &["K===K"], "T>=T2", &["V"]),
            "Invalid column name \"=K\"".to_string(),
        ),
        (
            request(10, 2, &["K"], "T>=T2", &["X="]),
            "Invalid column name: name can not be null or empty".to_string(),
        ),
    ];
    for (request, message) in steps {
        let refused = refusal(&mut c, Op::Aj(request.clone())).await;
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);
    }
    // and then it is accepted: none of the refusals used up the result ticket
    let response = call(&mut c, Op::Aj(request(10, 2, &["K"], "T>=T2", &["V"])))
        .await
        .unwrap();
    assert_eq!(response.size, 3);
}

#[tokio::test]
async fn what_the_as_of_join_refuses_is_an_internal_failure() {
    let mut c = Connected::start().await;
    stamped(&mut c).await;
    let refused = [
        // a column that a table does not have: stamp, exact match, addition
        Op::Aj(request(10, 2, &[], "T>=Nope", &["V"])),
        Op::Aj(request(11, 2, &[], "T2", &["V"])),
        Op::Raj(request(12, 2, &["Nope"], "T<=T2", &["V"])),
        Op::Aj(request(13, 2, &[], "T>=T2", &["Nope"])),
        // an added column named like a left column
        Op::Aj(request(14, 2, &[], "T>=T2", &["L=V"])),
        Op::Aj(request(15, 2, &[], "T>=T2", &[])),
        // stamps of different types
        Op::Aj(request(16, 2, &[], "T>=K", &["V"])),
        legacy(17, &["T=Nope"], &["V"], 0),
        // the deprecated request without a match: Java reads the last of none
        legacy(18, &[], &["V"], 0),
    ];
    for op in refused {
        assert_internal(&refusal(&mut c, op).await);
    }

    // in a batch the operation fails and the batch goes on
    let ops = vec![
        Op::Aj(request(20, 2, &[], "T>=Nope", &["V"])),
        Op::Aj(request(21, 2, &[], "T>=T2", &["V"])),
    ];
    let (responses, end) = c.batch(ops).await.unwrap();
    let responses = by_result(responses);
    assert_eq!(responses.len(), 2);
    assert!(!responses[0].success);
    assert!(responses[1].success);
    assert_internal(&end.expect("the stream ends with the failure"));
}

#[tokio::test]
async fn the_deprecated_as_of_join_reads_its_rule_the_other_way_around() {
    let mut c = Connected::start().await;
    stamped(&mut c).await;
    for (result, rule, expected) in [
        (10, MatchRule::LessThanEqual, ["1", "2", "3"]),
        (11, MatchRule::LessThan, ["", "2", "3"]),
        (12, MatchRule::GreaterThanEqual, ["1", "3", "4"]),
        (13, MatchRule::GreaterThan, ["2", "3", "4"]),
    ] {
        let response = call(&mut c, legacy(result, &["T=T2"], &["V"], rule as i32))
            .await
            .unwrap();
        assert_eq!(response.size, 3);
        let table = c.table(result);
        assert_eq!(column_names(table.as_ref()), ["K", "T", "L", "T2", "V"]);
        assert_eq!(values(table.as_ref(), "V"), texts(&expected), "{rule:?}");
    }

    // the last match is the stamps, the others are exact; the strings are read as the
    // other joins read them, so `==` is accepted in an addition
    let on = ["K", " T == T2 "];
    let greater = MatchRule::GreaterThanEqual as i32;
    call(&mut c, legacy(20, &on, &["Value==V"], greater))
        .await
        .unwrap();
    let table = c.table(20);
    assert_eq!(column_names(table.as_ref()), ["K", "T", "L", "T2", "Value"]);
    assert_eq!(values(table.as_ref(), "Value"), texts(&["1", "4", ""]));

    // a stamp column of one name, and no additions
    call(&mut c, legacy(21, &["K", "T=T2"], &[], 0))
        .await
        .unwrap();
    assert_eq!(
        column_names(c.table(21).as_ref()),
        ["K", "T", "L", "T2", "V"]
    );

    // the columns are checked before the rule, and an as-of match is not one of them
    for (op, message) in [
        (
            legacy(30, &["T=T2"], &["V"], 4),
            "Unrecognized as-of match rule",
        ),
        (legacy(31, &[], &[], -1), "Unrecognized as-of match rule"),
        (
            legacy(32, &["T=T2"], &["V V"], 99),
            "Unable to parse expression: \"V V\": V V",
        ),
        (
            legacy(33, &["K", "T>=T2"], &["V"], 0),
            "Unable to parse expression: \"T>=T2\": T>=T2",
        ),
        (
            legacy(34, &["T=in"], &["V"], 0),
            "Failed to get expression for all matched patterns: T=in",
        ),
    ] {
        let refused = refusal(&mut c, op.clone()).await;
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);
        let refused = c.batch(vec![op]).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert_eq!(refused.message(), message);
    }
}

#[tokio::test]
async fn an_as_of_join_of_a_ticking_table_is_refreshing() {
    let mut c = Connected::start().await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    let both = proto::AjRajTablesRequest {
        result_id: ticket(3),
        left_id: by_ticket(1),
        right_id: by_ticket(1),
        exact_match_columns: Vec::new(),
        as_of_column: "Timestamp".to_string(),
        columns_to_add: texts(&["Seen = Timestamp"]),
    };
    let response = call(&mut c, Op::Aj(both)).await.unwrap();
    assert!(!response.is_static);
    let result = c.table(3);
    let graph = c.server.state.graph.clone();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    let (size, stamps, seen) = locked(graph.as_ref(), || {
        (
            result.row_set().size(),
            values(result.as_ref(), "Timestamp"),
            values(result.as_ref(), "Seen"),
        )
    });
    assert!(size > 0);
    assert_eq!(stamps, seen);
}
