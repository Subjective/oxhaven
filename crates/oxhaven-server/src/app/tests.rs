//! Tests of the applications, their tickets and the demo application. What Java answers to
//! `ListFields` and to application and scope tickets is replayed in `crate::ide`; the tests
//! here cover what could not be recorded there: fields that change while a client listens,
//! which on Java only an application script can do.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, TimestampNanosecondArray};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::empty_table;
use oxhaven_proto::grpc::application_service_client::ApplicationServiceClient;
use oxhaven_proto::grpc::{FieldInfo, FieldsChangeUpdate, ListFieldsRequest};
use tokio_stream::StreamExt;
use tonic::{Code, Streaming};

use super::*;
use crate::ide::{demo_server, run_cycle, ManualClock};
use crate::session::ExportValue;
use crate::test_support::{with_auth, TestServer};

fn table(size: u64) -> ExportValue {
    ExportValue::Table(empty_table(size))
}

async fn list_fields(server: &TestServer, bearer: &str) -> Streaming<FieldsChangeUpdate> {
    ApplicationServiceClient::new(server.channel.clone())
        .list_fields(with_auth(ListFieldsRequest {}, bearer))
        .await
        .unwrap()
        .into_inner()
}

async fn next(updates: &mut Streaming<FieldsChangeUpdate>) -> FieldsChangeUpdate {
    tokio::time::timeout(Duration::from_secs(5), updates.next())
        .await
        .expect("a message within five seconds")
        .expect("an open stream")
        .unwrap()
}

/// `name: type: description` of each field.
fn described(fields: &[FieldInfo]) -> Vec<String> {
    fields
        .iter()
        .map(|field| {
            let ticket = field.typed_ticket.as_ref().unwrap();
            assert_eq!(
                ticket.ticket.as_ref().unwrap().ticket,
                application_field_ticket(&field.application_id, &field.field_name)
            );
            assert_eq!(field.application_name, "Trading");
            format!(
                "{}: {}: {}",
                field.field_name, ticket.r#type, field.field_description
            )
        })
        .collect()
}

#[tokio::test]
async fn a_listening_client_is_told_of_fields_that_are_set_replaced_and_removed() {
    let server = TestServer::start().await;
    let bearer = server.login().await;
    let app = server
        .state
        .applications
        .create("trading", "Trading")
        .unwrap();
    app.set_field("quotes", table(1)).unwrap();

    let mut updates = list_fields(&server, &bearer).await;
    let initial = next(&mut updates).await;
    assert_eq!(described(&initial.created), ["quotes: Table: "]);
    assert!(initial.updated.is_empty() && initial.removed.is_empty());

    // Changes made together arrive as one message: a new field is created, a replaced one
    // updated, and one that came and went is not mentioned.
    app.set_field_described("trades", table(2), "Every trade")
        .unwrap();
    app.set_field("quotes", ExportValue::object(7_u8)).unwrap();
    app.set_field("scratch", table(3)).unwrap();
    assert!(app.remove_field("scratch"));
    let first = next(&mut updates).await;
    assert_eq!(described(&first.created), ["trades: Table: Every trade"]);
    // an object that is no table has no type: this server has no object type plugins
    assert_eq!(described(&first.updated), ["quotes: : "]);
    assert!(first.removed.is_empty());

    // The next message waits until a quarter of a second after the last one. A removed
    // field is described as the client knew it; removed and set again is an update.
    let sent = Instant::now();
    assert!(app.remove_field("quotes"));
    assert!(app.remove_field("trades"));
    app.set_field("trades", table(4)).unwrap();
    assert!(!app.remove_field("never set"));
    let second = next(&mut updates).await;
    assert!(
        sent.elapsed() >= Duration::from_millis(200),
        "sent after {:?}",
        sent.elapsed()
    );
    assert!(second.created.is_empty());
    assert_eq!(described(&second.updated), ["trades: Table: "]);
    assert_eq!(described(&second.removed), ["quotes: : "]);

    // a client that connects now is told what there is, in the order it came to be
    let mut late = list_fields(&server, &bearer).await;
    let listed = next(&mut late).await;
    assert_eq!(described(&listed.created), ["trades: Table: "]);
    assert_eq!(app.field_names(), ["trades"]);
}

#[tokio::test]
async fn fields_set_while_nobody_listens_are_listed_in_the_order_they_were_set() {
    let server = TestServer::start().await;
    let bearer = server.login().await;
    let app = server
        .state
        .applications
        .create("trading", "Trading")
        .unwrap();
    for name in ["zeta", "alpha", "mid"] {
        app.set_field(name, table(1)).unwrap();
    }
    // replacing keeps the place, removing and setting again moves to the end
    app.set_field("zeta", table(2)).unwrap();
    app.remove_field("alpha");
    app.set_field("alpha", table(3)).unwrap();

    let mut updates = list_fields(&server, &bearer).await;
    let initial = next(&mut updates).await;
    assert_eq!(
        described(&initial.created),
        ["zeta: Table: ", "mid: Table: ", "alpha: Table: "]
    );
}

#[tokio::test]
async fn a_stream_ends_with_its_client_and_with_its_session() {
    let server = TestServer::start().await;
    let bearer = server.login().await;
    let session = server.session(&bearer);
    let applications = &server.state.applications;

    let updates = applications.subscribe(&session).unwrap();
    assert_eq!(applications.subscription_count(), 1);
    drop(updates);
    for _ in 0..100 {
        if applications.subscription_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(applications.subscription_count(), 0);

    let mut updates = applications.subscribe(&session).unwrap();
    assert!(updates.recv().await.unwrap().is_ok());
    server.state.sessions.close_session(&session);
    let status = updates.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::Cancelled);
    assert_eq!(status.message(), "subscription cancelled");
    assert_eq!(applications.subscription_count(), 0);

    // a session that has ended cannot listen
    let refused = applications.subscribe(&session).unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn applications_and_fields_are_checked() {
    let applications = Applications::new();
    let app = applications.create("trading", "Trading").unwrap();
    assert_eq!((app.id(), app.name().as_str()), ("trading", "Trading"));
    assert_eq!(
        applications.create("trading", "Again").err(),
        Some(ApplicationError::DuplicateId("trading".to_string()))
    );
    assert_eq!(
        applications.create("scope", "Scope").err(),
        Some(ApplicationError::ReservedId)
    );
    assert!(applications.get("trading").is_some());
    assert!(applications.get("other").is_none());

    assert_eq!(
        app.set_field("caf\u{e9}", table(1)),
        Err(ApplicationError::InvalidFieldName("caf\u{e9}".to_string()))
    );
    assert_eq!(
        app.set_field("tab\there", table(1)),
        Err(ApplicationError::InvalidFieldName("tab\there".to_string()))
    );
    assert_eq!(
        app.set_field_described("quotes", table(1), ""),
        Err(ApplicationError::EmptyDescription)
    );
    assert!(app.field_names().is_empty());
    // a name is not otherwise restricted, and one with a slash can still be resolved
    app.set_field("a b/c", table(1)).unwrap();
    assert!(app.field("a b/c").is_some());
}

#[tokio::test]
async fn a_field_ticket_names_the_value_the_field_has_now() {
    let server = TestServer::start().await;
    let app = server
        .state
        .applications
        .create("trading", "Trading")
        .unwrap();
    let tickets = &server.state.tickets;
    let ticket = application_field_ticket("trading", "quotes");
    assert_eq!(ticket, b"a/trading/f/quotes");
    let size = |ticket: &[u8]| {
        let export = tickets.resolve(None, ticket, "id").unwrap();
        export
            .value()
            .map(|value| value.as_table().unwrap().row_set().size())
            .ok_or_else(|| export.failure().unwrap().cause.unwrap().code())
    };

    assert_eq!(size(&ticket), Err(Code::NotFound));
    app.set_field("quotes", table(4)).unwrap();
    assert_eq!(size(&ticket), Ok(4));
    app.set_field("quotes", table(9)).unwrap();
    assert_eq!(size(&ticket), Ok(9));
    app.remove_field("quotes");
    assert_eq!(size(&ticket), Err(Code::NotFound));
    assert_eq!(
        tickets.log_name(&ticket, "id").unwrap(),
        "app/trading/field/quotes"
    );
}

#[tokio::test]
async fn a_scope_ticket_names_the_variable_of_that_name() {
    let server = TestServer::start().await;
    let scope = &server.state.scope;
    let tickets = &server.state.tickets;
    assert_eq!(scope_ticket("t"), b"s/t");

    scope.put("t", table(6)).unwrap();
    scope.put("u", table(1)).unwrap();
    assert_eq!(scope.names(), ["t", "u"]);
    let export = tickets.resolve(None, b"s/t", "id").unwrap();
    assert_eq!(
        export.value().unwrap().as_table().unwrap().row_set().size(),
        6
    );
    assert_eq!(tickets.log_name(b"s/t", "id").unwrap(), "scope/t");

    assert!(scope.remove("t").is_some());
    assert!(scope.get("t").is_none());
    let export = tickets.resolve(None, b"s/t", "id").unwrap();
    assert_eq!(
        export.failure().unwrap().cause.unwrap().code(),
        Code::NotFound
    );
}

fn longs(table: &Arc<dyn Table>, column: usize) -> Vec<i64> {
    let values = table.column(column).chunk(&table.row_set()).unwrap();
    match table.definition().columns()[column].column_type {
        ColumnType::Instant => values
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .values()
            .to_vec(),
        _ => {
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.null_count(), 0);
            values.values().to_vec()
        }
    }
}

#[tokio::test]
async fn the_ticking_table_is_the_last_three_rows_of_a_half_second_counter() {
    let (server, clock) = demo_server().await;
    let app = server.state.applications.get(demo::APPLICATION_ID).unwrap();
    assert_eq!(app.name(), demo::APPLICATION_NAME);
    assert_eq!(app.field_names(), [demo::STATIC_FIELD, demo::TICKING_FIELD]);
    let ticking = app
        .field(demo::TICKING_FIELD)
        .unwrap()
        .as_table()
        .unwrap()
        .clone();

    let columns: Vec<_> = ticking
        .definition()
        .columns()
        .iter()
        .map(|column| (column.name.as_str(), column.column_type.clone()))
        .collect();
    assert_eq!(
        columns,
        [
            ("Timestamp", ColumnType::Instant),
            ("N", ColumnType::Long),
            ("Sq", ColumnType::Long)
        ]
    );
    assert!(ticking.is_refreshing());

    let half_second = 500_000_000;
    let start = ManualClock::START;
    assert_eq!(longs(&ticking, 1), [0, 1, 2]);
    assert_eq!(longs(&ticking, 2), [0, 1, 4]);
    assert_eq!(
        longs(&ticking, 0),
        [start, start + half_second, start + 2 * half_second]
    );

    // two more half seconds have passed since the last cycle, and then seven more
    run_cycle(&server).await;
    assert_eq!(longs(&ticking, 1), [2, 3, 4]);
    clock.advance(Duration::from_millis(3_500));
    run_cycle(&server).await;
    assert_eq!(longs(&ticking, 1), [9, 10, 11]);
    assert_eq!(longs(&ticking, 2), [81, 100, 121]);
    assert_eq!(
        longs(&ticking, 0),
        [9, 10, 11].map(|row| start + row * half_second)
    );
    assert_eq!(ticking.row_set().size(), 3);
}

#[tokio::test]
async fn the_demo_application_cannot_be_installed_twice() {
    let (server, _clock) = demo_server().await;
    let again = demo::install(&server.state.applications, &server.state.op_context());
    assert_eq!(
        again.err().unwrap(),
        "Duplicate application found for app_id ide_app"
    );
}
