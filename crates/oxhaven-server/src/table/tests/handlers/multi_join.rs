//! `MultiJoinTables`, called as a client calls it. Every status and message here is what
//! the Java server (Deephaven 42.9) answered for the same request.

use super::*;

/// Table 1: `K` = 1, 2, 4; `A` = a1, a2, a4.
/// Table 2: `K` = 2, 3, 1; `B` = 20, 30, 10.
/// Table 3: `Key` = 3, 5; `C` = 300, 500; `Extra` = x, y.
/// Table 4, with a key twice: `K` = 1, 1; `D` = 1, 2.
/// Table 5, with a key of another type: `K` = one; `S` = 1.
async fn keyed(c: &mut Connected) {
    table_of(
        c,
        1,
        &[
            &["K = 1", "A = `a1`"],
            &["K = 2", "A = `a2`"],
            &["K = 4", "A = `a4`"],
        ],
    )
    .await;
    table_of(
        c,
        2,
        &[
            &["K = 2", "B = 20"],
            &["K = 3", "B = 30"],
            &["K = 1", "B = 10"],
        ],
    )
    .await;
    table_of(
        c,
        3,
        &[
            &["Key = 3", "C = 300", "Extra = `x`"],
            &["Key = 5", "C = 500", "Extra = `y`"],
        ],
    )
    .await;
    table_of(c, 4, &[&["K = 1", "D = 1"], &["K = 1", "D = 2"]]).await;
    table_of(c, 5, &[&["K = `one`", "S = 1"]]).await;
}

fn input(source: Option<TableReference>, on: &[&str], add: &[&str]) -> proto::MultiJoinInput {
    proto::MultiJoinInput {
        source_id: source,
        columns_to_match: texts(on),
        columns_to_add: texts(add),
    }
}

fn of(table: i32, on: &[&str], add: &[&str]) -> proto::MultiJoinInput {
    input(by_ticket(table), on, add)
}

fn joined(result: i32, inputs: Vec<proto::MultiJoinInput>) -> Op {
    Op::MultiJoin(proto::MultiJoinTablesRequest {
        result_id: ticket(result),
        multi_join_inputs: inputs,
    })
}

async fn refusal(c: &mut Connected, op: Op) -> Status {
    call(c, op).await.unwrap_err()
}

#[tokio::test]
async fn multi_join_has_a_row_for_every_key_of_any_table() {
    let mut c = Connected::start().await;
    keyed(&mut c).await;

    // the keys in the order they are first seen, table by table
    let op = joined(10, vec![of(1, &["K"], &[]), of(2, &["K"], &[])]);
    let response = call(&mut c, op).await.unwrap();
    assert!(response.success);
    assert!(response.is_static);
    assert_eq!(response.size, 4);
    let table = c.table(10);
    assert_eq!(column_names(table.as_ref()), ["K", "A", "B"]);
    assert_eq!(values(table.as_ref(), "K"), ints(&[1, 2, 4, 3]));
    assert_eq!(values(table.as_ref(), "A"), texts(&["a1", "a2", "a4", ""]));
    assert_eq!(values(table.as_ref(), "B"), texts(&["10", "20", "", "30"]));

    let op = joined(11, vec![of(2, &["K"], &[]), of(1, &["K"], &[])]);
    call(&mut c, op).await.unwrap();
    let table = c.table(11);
    assert_eq!(column_names(table.as_ref()), ["K", "B", "A"]);
    assert_eq!(values(table.as_ref(), "K"), ints(&[2, 3, 1, 4]));

    // a key renamed, columns chosen and renamed, spelled as `JoinMatch#parse` and
    // `JoinAddition#parse` read them
    let inputs = vec![
        of(1, &["  K  "], &["  Name=A "]),
        of(2, &["K"], &[]),
        of(3, &[" K ==  Key "], &[" C "]),
    ];
    let response = call(&mut c, joined(12, inputs)).await.unwrap();
    assert_eq!(response.size, 5);
    let table = c.table(12);
    assert_eq!(column_names(table.as_ref()), ["K", "Name", "B", "C"]);
    assert_eq!(values(table.as_ref(), "K"), ints(&[1, 2, 4, 3, 5]));
    assert_eq!(
        values(table.as_ref(), "C"),
        texts(&["", "", "", "300", "500"])
    );

    // the first input names the keys; one table alone; a table with itself
    let inputs = vec![of(3, &["Id=Key"], &["C"]), of(1, &["Id=K"], &[])];
    call(&mut c, joined(13, inputs)).await.unwrap();
    assert_eq!(column_names(c.table(13).as_ref()), ["Id", "C", "A"]);
    let response = call(&mut c, joined(14, vec![of(1, &["K"], &[])]))
        .await
        .unwrap();
    assert_eq!(response.size, 3);
    let inputs = vec![of(1, &["K"], &[]), of(1, &["K"], &["Again=A", "Copy=K"])];
    call(&mut c, joined(15, inputs)).await.unwrap();
    let table = c.table(15);
    assert_eq!(column_names(table.as_ref()), ["K", "A", "Again", "Copy"]);
    assert_eq!(values(table.as_ref(), "Again"), texts(&["a1", "a2", "a4"]));
}

#[tokio::test]
async fn multi_join_checks_the_request_in_javas_order() {
    let mut c = Connected::start().await;
    keyed(&mut c).await;
    let no_matches = "io.deephaven.proto.backplane.grpc.MultiJoinInput must have at least one \
                      columns_to_match (2)";
    let no_ref = "io.deephaven.proto.backplane.grpc.TableReference must have oneof ref. Note: \
                  this may also indicate that the server is older than the client and doesn't \
                  know about this new oneof option.";
    let unset = Some(TableReference { r#ref: None });
    let empty_ticket = Some(TableReference {
        r#ref: Some(Ref::Ticket(Ticket { ticket: Vec::new() })),
    });
    let cases: Vec<(Vec<proto::MultiJoinInput>, &str)> = vec![
        (Vec::new(), "Cannot join zero source tables."),
        (vec![of(1, &[], &[])], no_matches),
        (vec![of(1, &["K"], &[]), of(2, &[], &["B"])], no_matches),
        (vec![input(None, &["K"], &[])], no_ref),
        (vec![of(1, &["K"], &[]), input(None, &["K"], &[])], no_ref),
        (vec![input(unset, &["K"], &[])], no_ref),
        (
            vec![input(empty_ticket.clone(), &["K"], &[])],
            "Ticket is empty",
        ),
        // an input is checked whole before the next: its matches, then its table
        (vec![input(None, &[], &[])], no_matches),
        (
            vec![input(empty_ticket.clone(), &[], &[]), input(None, &[], &[])],
            no_matches,
        ),
        (
            vec![input(empty_ticket, &["K"], &[]), of(1, &[], &[])],
            "Ticket is empty",
        ),
        // the columns are read when the join is made, after every input is checked
        (vec![of(1, &["A B"], &[]), of(2, &[], &[])], no_matches),
    ];
    for (unused, (inputs, message)) in (900..).zip(cases) {
        let op = joined(10, inputs);
        let refused = refusal(&mut c, op.clone()).await;
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);

        // in a batch, before any operation of the batch is created. The export named
        // before the refused operation stays claimed, so each batch names another
        let refused = c
            .batch(vec![Op::EmptyTable(empty(ticket(unused), 1)), op])
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert_eq!(refused.message(), message);
        let unused = c.server.session(&c.bearer).get_export(unused).unwrap();
        assert_ne!(unused.state(), ExportState::Exported);
    }

    let refused = refusal(&mut c, joined(10, vec![input(by_offset(0), &["K"], &[])])).await;
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "One-shot operations must use ticket references"
    );

    // and then it is accepted: none of the refusals used up the result ticket
    let op = joined(10, vec![of(1, &["K"], &[]), of(2, &["K"], &[])]);
    assert_eq!(call(&mut c, op).await.unwrap().size, 4);
}

#[tokio::test]
async fn what_the_multi_join_refuses_is_an_internal_failure() {
    let mut c = Connected::start().await;
    keyed(&mut c).await;
    let k = |table: i32| of(table, &["K"], &[]);
    let refused = [
        // columns that cannot be read: matches and additions
        vec![of(1, &[""], &[]), k(2)],
        vec![of(1, &["A B"], &[]), k(2)],
        vec![of(1, &["K="], &[]), k(2)],
        vec![of(1, &["K===K"], &[]), k(2)],
        vec![of(1, &["in=K"], &[]), k(2)],
        vec![k(1), of(2, &["K B"], &[])],
        vec![of(1, &["K"], &[""]), k(2)],
        vec![of(1, &["K"], &["X="]), k(2)],
        vec![of(1, &["K"], &["X = A + 1"]), k(2)],
        // keys that differ in number, in name and in type
        vec![k(1), of(2, &["K", "B"], &[])],
        vec![k(1), of(2, &["J=K"], &[])],
        vec![k(1), k(5)],
        vec![of(1, &["X=K", "X=A"], &[]), of(2, &["X=B"], &[])],
        // a column added twice, or named as a key
        vec![k(1), k(1)],
        vec![of(1, &["K"], &["K=A"])],
        vec![of(1, &["K"], &["X=A", "X=K"])],
        // a key that a table has twice
        vec![k(4), k(2)],
        vec![k(2), k(4)],
        // columns that a table does not have
        vec![of(1, &["Nope"], &[]), k(2)],
        vec![k(1), of(2, &["K=Nope"], &[])],
        vec![of(1, &["K"], &["Nope"]), k(2)],
        vec![of(1, &["K"], &["X=Nope"]), k(2)],
    ];
    for (at, inputs) in refused.into_iter().enumerate() {
        let status = refusal(&mut c, joined(10 + at as i32, inputs)).await;
        assert_internal(&status);
    }

    // in a batch the operation fails and the batch goes on
    let ops = vec![joined(50, vec![k(4), k(2)]), joined(51, vec![k(1), k(2)])];
    let (responses, end) = c.batch(ops).await.unwrap();
    let responses = by_result(responses);
    assert_eq!(responses.len(), 2);
    assert!(!responses[0].success);
    assert!(responses[1].success);
    assert_internal(&end.expect("the stream ends with the failure"));
}

#[tokio::test]
async fn multi_join_runs_in_a_batch_on_tables_of_the_batch() {
    let mut c = Connected::start().await;
    keyed(&mut c).await;
    let ops = vec![
        fetch(by_ticket(1), None),
        fetch(by_ticket(2), None),
        Op::MultiJoin(proto::MultiJoinTablesRequest {
            result_id: None,
            multi_join_inputs: vec![
                input(by_offset(0), &["K"], &[]),
                input(by_offset(1), &["K"], &[]),
            ],
        }),
        joined(
            20,
            vec![
                input(by_offset(2), &["Key=K"], &["B"]),
                of(3, &["Key"], &["C"]),
            ],
        ),
    ];
    let (responses, end) = c.batch(ops).await.unwrap();
    assert!(end.is_none(), "{end:?}");
    assert_eq!(responses.len(), 4);
    assert!(responses.iter().all(|response| response.success));
    let table = c.table(20);
    assert_eq!(column_names(table.as_ref()), ["Key", "B", "C"]);
    assert_eq!(values(table.as_ref(), "Key"), ints(&[1, 2, 4, 3, 5]));
    assert_eq!(
        values(table.as_ref(), "C"),
        texts(&["", "", "", "300", "500"])
    );
}

#[tokio::test]
async fn a_multi_join_of_a_ticking_table_follows_it() {
    let mut c = Connected::start().await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    let inputs = vec![
        of(1, &["Timestamp"], &["First = Timestamp"]),
        of(1, &["Timestamp"], &["Second = Timestamp"]),
    ];
    let response = call(&mut c, joined(3, inputs)).await.unwrap();
    assert!(!response.is_static);
    let result = c.table(3);
    let source = c.table(1);
    let graph = c.server.state.graph.clone();
    tokio::time::sleep(Duration::from_millis(5)).await;
    c.cycle().await;
    let (size, expected, stamps, first, second) = locked(graph.as_ref(), || {
        (
            result.row_set().size(),
            values(source.as_ref(), "Timestamp"),
            values(result.as_ref(), "Timestamp"),
            values(result.as_ref(), "First"),
            values(result.as_ref(), "Second"),
        )
    });
    assert!(size > 0);
    assert_eq!(stamps, expected);
    assert_eq!(first, expected);
    assert_eq!(second, expected);
}
