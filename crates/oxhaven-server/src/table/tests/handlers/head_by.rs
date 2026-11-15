//! `HeadBy` and `TailBy`, called as a client calls them. Every status and message here is
//! what the Java server (Deephaven 42.9) answered for the same request.

use super::*;

type Wrap = fn(proto::HeadOrTailByRequest) -> Op;

fn request(result: i32, source: i32, rows: i64, by: &[&str]) -> proto::HeadOrTailByRequest {
    proto::HeadOrTailByRequest {
        result_id: ticket(result),
        source_id: by_ticket(source),
        num_rows: rows,
        group_by_column_specs: texts(by),
    }
}

#[tokio::test]
async fn head_by_and_tail_by_keep_the_rows_at_either_end_of_each_group() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;

    // the group-by columns first, then the others; group after group
    let response = call(&mut c, Op::HeadBy(request(10, 1, 2, &["S"])))
        .await
        .unwrap();
    assert!(response.success);
    assert!(response.is_static);
    assert_eq!(response.size, 4);
    let table = c.table(10);
    assert_eq!(column_names(table.as_ref()), ["S", "I", "N"]);
    assert_eq!(values(table.as_ref(), "S"), texts(&["a", "a", "b", "b"]));
    assert_eq!(values(table.as_ref(), "I"), ints(&[0, 2, 1, 3]));

    let response = call(&mut c, Op::TailBy(request(11, 1, 2, &["S"])))
        .await
        .unwrap();
    assert!(response.is_static);
    assert_eq!(response.size, 4);
    let table = c.table(11);
    assert_eq!(column_names(table.as_ref()), ["S", "I", "N"]);
    assert_eq!(values(table.as_ref(), "I"), ints(&[2, 4, 3, 5]));

    // no group-by columns: the table is one group
    let response = call(&mut c, Op::HeadBy(request(12, 1, 1, &[])))
        .await
        .unwrap();
    assert_eq!(response.size, 1);
    let table = c.table(12);
    assert_eq!(column_names(table.as_ref()), ["I", "N", "S"]);
    assert_eq!(values(table.as_ref(), "I"), ints(&[0]));
    call(&mut c, Op::TailBy(request(13, 1, 1, &[])))
        .await
        .unwrap();
    assert_eq!(values(c.table(13).as_ref(), "I"), ints(&[5]));

    // several columns, in the order of the request; more rows than any group has
    let response = call(&mut c, Op::TailBy(request(14, 1, i64::MAX, &["S", "N"])))
        .await
        .unwrap();
    assert_eq!(response.size, 6);
    let table = c.table(14);
    assert_eq!(column_names(table.as_ref()), ["S", "N", "I"]);
    assert_eq!(values(table.as_ref(), "I"), ints(&[0, 1, 2, 3, 4, 5]));

    // in a batch, of a table the batch made: the first four rows, two of each letter
    for (result, wrap, kept) in [
        (15, Op::HeadBy as Wrap, [0, 1]),
        (16, Op::TailBy as Wrap, [2, 3]),
    ] {
        let (responses, status) = c
            .batch(vec![
                Op::Head(proto::HeadOrTailRequest {
                    result_id: None,
                    source_id: by_ticket(1),
                    num_rows: 4,
                }),
                wrap(proto::HeadOrTailByRequest {
                    source_id: by_offset(0),
                    ..request(result, 0, 1, &["S"])
                }),
            ])
            .await
            .unwrap();
        assert!(status.is_none(), "{status:?}");
        assert!(responses.iter().all(|response| response.success));
        assert_eq!(values(c.table(result).as_ref(), "I"), ints(&kept));
    }
}

#[tokio::test]
async fn head_by_and_tail_by_refuse_as_java_does() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let blink = TimeTableRequest {
        blink_table: true,
        ..ticking_time_table(ticket(2))
    };
    call(&mut c, Op::TimeTable(blink)).await.unwrap();

    for (base, wrap) in [(0, Op::HeadBy as Wrap), (100, Op::TailBy as Wrap)] {
        // a negative number of rows is refused with a status of its own, before the
        // columns and the source are looked at
        for (rows, by, source) in [
            (-1, &["S"][..], 1),
            (i64::MIN, &[][..], 1),
            (-1, &["Nope"][..], 1),
            (-1, &["Timestamp"][..], 2),
        ] {
            let message = format!("numRows must be >= 0 (found: {rows})");
            let op = wrap(request(10, source, rows, by));
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

        // what the engine refuses is an exception in Java: no rows, a column named twice,
        // a column that does not exist, a name with a space around it, a formula, a string
        // that is no formula, every column, more columns than there are, and a blink table
        let refusals: [(i32, i64, &[&str]); 14] = [
            (1, 0, &[]),
            (1, 0, &["S"]),
            (1, 1, &["S", "S"]),
            (1, 1, &["Nope"]),
            (1, 1, &["S", "Nope"]),
            (1, 1, &[" S"]),
            (1, 1, &["S "]),
            (1, 1, &["X = S"]),
            (1, 1, &["S = S"]),
            (1, 1, &["S +"]),
            (1, 1, &[""]),
            (1, 1, &["I", "N", "S"]),
            (1, 1, &["I", "N", "S", "I"]),
            (2, 1, &[]),
        ];
        for (result, (source, rows, by)) in (base + 30..).zip(refusals) {
            let refused = call(&mut c, wrap(request(result, source, rows, by)))
                .await
                .unwrap_err();
            assert_internal(&refused);
        }

        // in a batch the operations before it are made, and it fails the batch
        let (responses, status) = c
            .batch(vec![
                Op::EmptyTable(empty(ticket(base + 21), 1)),
                wrap(request(base + 22, 1, 0, &["S"])),
            ])
            .await
            .unwrap();
        let responses = by_result(responses);
        assert!(responses[0].success);
        assert!(!responses[1].success);
        assert_internal(&status.unwrap());
    }
}

#[tokio::test]
async fn a_head_by_or_tail_by_of_a_refreshing_table_follows_it() {
    let mut c = Connected::start().await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    call(&mut c, Op::Update(formulas(1, 2, &["K = 1"])))
        .await
        .unwrap();
    let head = call(&mut c, Op::HeadBy(request(3, 2, 1, &["K"])))
        .await
        .unwrap();
    let tail = call(&mut c, Op::TailBy(request(4, 2, 1, &["K"])))
        .await
        .unwrap();
    assert!(!head.is_static);
    assert!(!tail.is_static);
    let (source, head, tail) = (c.table(2), c.table(3), c.table(4));
    assert_eq!(column_names(tail.as_ref()), ["K", "Timestamp"]);
    let graph = c.server.state.graph.clone();
    let read = || {
        locked(graph.as_ref(), || {
            (
                values(source.as_ref(), "Timestamp"),
                values(head.as_ref(), "Timestamp"),
                values(tail.as_ref(), "Timestamp"),
            )
        })
    };
    for expected in [1, 2] {
        // the time table has its first row after the first cycle and more after the next
        tokio::time::sleep(Duration::from_millis(5)).await;
        c.cycle().await;
        let (stamps, first, last) = read();
        assert!(stamps.len() >= expected, "{stamps:?}");
        assert_eq!(first, stamps[..1]);
        assert_eq!(last, stamps[stamps.len() - 1..]);
    }
}
