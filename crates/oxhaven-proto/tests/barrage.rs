//! Build-then-read round trips for the Barrage flatbuffers messages, and a check that the
//! checked-in generated code still corresponds to the vendored schema.
//!
//! No Java counterpart. The nesting used here, an inner message finished as its own flatbuffer
//! and carried in `BarrageMessageWrapper.msg_payload`, is how the reference reads them, for
//! example in `io.deephaven.server.arrow.BarrageSubscriptionRequestHandler` and
//! `io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl`.

use flatbuffers::{FlatBufferBuilder, Vector, WIPOffset};
use oxhaven_proto::barrage::{
    BarrageMessageType, BarrageMessageWrapper, BarrageMessageWrapperArgs, BarrageModColumnMetadata,
    BarrageModColumnMetadataArgs, BarrageSnapshotOptions, BarrageSnapshotOptionsArgs,
    BarrageSnapshotRequest, BarrageSnapshotRequestArgs, BarrageSubscriptionOptions,
    BarrageSubscriptionOptionsArgs, BarrageSubscriptionRequest, BarrageSubscriptionRequestArgs,
    BarrageUpdateMetadata, BarrageUpdateMetadataArgs, MAGIC,
};

/// Writes `bytes` as a flatbuffers `[byte]` vector.
fn byte_vector<'a>(builder: &mut FlatBufferBuilder<'a>, bytes: &[u8]) -> WIPOffset<Vector<'a, i8>> {
    let signed: Vec<i8> = bytes.iter().map(|&b| i8::from_ne_bytes([b])).collect();
    builder.create_vector(&signed)
}

/// Wraps a finished inner message in a `BarrageMessageWrapper` and returns the finished bytes.
fn wrap(msg_type: BarrageMessageType, payload: &[u8]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let msg_payload = byte_vector(&mut builder, payload);
    let wrapper = BarrageMessageWrapper::create(
        &mut builder,
        &BarrageMessageWrapperArgs {
            magic: MAGIC,
            msg_type,
            msg_payload: Some(msg_payload),
        },
    );
    builder.finish(wrapper, None);
    builder.finished_data().to_vec()
}

/// Reads a wrapper, checks its magic and type, and returns its payload.
fn unwrap(bytes: &[u8], expected: BarrageMessageType) -> &[u8] {
    let wrapper = flatbuffers::root::<BarrageMessageWrapper>(bytes).expect("valid wrapper");
    assert_eq!(wrapper.magic(), MAGIC);
    assert_eq!(wrapper.msg_type(), expected);
    wrapper.msg_payload().expect("payload present").bytes()
}

#[test]
fn magic_is_ascii_dphn() {
    // The schema: "It is the numerical representation of the ASCII "dphn"."
    assert_eq!(MAGIC.to_le_bytes(), *b"dphn");
}

#[test]
fn message_type_values_match_the_schema() {
    assert_eq!(BarrageMessageType::None.0, 0);
    assert_eq!(BarrageMessageType::BarrageSerializationOptions.0, 4);
    assert_eq!(BarrageMessageType::BarrageSubscriptionRequest.0, 5);
    assert_eq!(BarrageMessageType::BarrageUpdateMetadata.0, 6);
    assert_eq!(BarrageMessageType::BarrageSnapshotRequest.0, 7);
    assert_eq!(BarrageMessageType::BarragePublicationRequest.0, 8);
}

#[test]
fn subscription_request_round_trips_through_the_wrapper() {
    let ticket = [b'e', 1, 0, 0, 0];
    let columns = [0b0000_0101];
    let viewport = [0x80, 0x7f, 0x00, 0xff];
    let token = [7, 7, 7];

    let mut builder = FlatBufferBuilder::new();
    let ticket_offset = byte_vector(&mut builder, &ticket);
    let columns_offset = byte_vector(&mut builder, &columns);
    let viewport_offset = byte_vector(&mut builder, &viewport);
    let token_offset = byte_vector(&mut builder, &token);
    let options = BarrageSubscriptionOptions::create(
        &mut builder,
        &BarrageSubscriptionOptionsArgs {
            use_deephaven_nulls: true,
            min_update_interval_ms: 250,
            batch_size: 4096,
            max_message_size: 100_000_000,
            columns_as_list: true,
            preview_list_length_limit: -5,
        },
    );
    let request = BarrageSubscriptionRequest::create(
        &mut builder,
        &BarrageSubscriptionRequestArgs {
            ticket: Some(ticket_offset),
            columns: Some(columns_offset),
            viewport: Some(viewport_offset),
            subscription_options: Some(options),
            reverse_viewport: true,
            subscription_token: Some(token_offset),
        },
    );
    builder.finish(request, None);

    let wrapped = wrap(
        BarrageMessageType::BarrageSubscriptionRequest,
        builder.finished_data(),
    );
    let payload = unwrap(&wrapped, BarrageMessageType::BarrageSubscriptionRequest);
    let read = flatbuffers::root::<BarrageSubscriptionRequest>(payload).expect("valid request");

    assert_eq!(read.ticket().expect("ticket").bytes(), ticket);
    assert_eq!(read.columns().expect("columns").bytes(), columns);
    assert_eq!(read.viewport().expect("viewport").bytes(), viewport);
    assert_eq!(read.subscription_token().expect("token").bytes(), token);
    assert!(read.reverse_viewport());
    let options = read.subscription_options().expect("options");
    assert!(options.use_deephaven_nulls());
    assert_eq!(options.min_update_interval_ms(), 250);
    assert_eq!(options.batch_size(), 4096);
    assert_eq!(options.max_message_size(), 100_000_000);
    assert!(options.columns_as_list());
    assert_eq!(options.preview_list_length_limit(), -5);
}

#[test]
fn absent_fields_read_as_none_or_schema_defaults() {
    let mut builder = FlatBufferBuilder::new();
    let request = BarrageSubscriptionRequest::create(
        &mut builder,
        &BarrageSubscriptionRequestArgs::default(),
    );
    builder.finish(request, None);
    let read = flatbuffers::root::<BarrageSubscriptionRequest>(builder.finished_data())
        .expect("valid request");

    assert!(read.ticket().is_none());
    assert!(read.columns().is_none());
    assert!(read.viewport().is_none());
    assert!(read.subscription_options().is_none());
    assert!(read.subscription_token().is_none());
    assert!(!read.reverse_viewport());
}

#[test]
fn update_metadata_round_trips_through_the_wrapper() {
    let added = [1, 2, 3];
    let removed = [4];
    let shifts = [5, 6];
    let included = [1, 2];
    let effective_viewport = [9, 9];
    let effective_columns = [0b11];
    let modified: [&[u8]; 2] = [&[10, 11], &[]];

    let mut builder = FlatBufferBuilder::new();
    let mod_nodes: Vec<_> = modified
        .iter()
        .map(|rows| {
            let rows = byte_vector(&mut builder, rows);
            BarrageModColumnMetadata::create(
                &mut builder,
                &BarrageModColumnMetadataArgs {
                    modified_rows: Some(rows),
                },
            )
        })
        .collect();
    let mod_nodes = builder.create_vector(&mod_nodes);
    let added_offset = byte_vector(&mut builder, &added);
    let removed_offset = byte_vector(&mut builder, &removed);
    let shifts_offset = byte_vector(&mut builder, &shifts);
    let included_offset = byte_vector(&mut builder, &included);
    let viewport_offset = byte_vector(&mut builder, &effective_viewport);
    let columns_offset = byte_vector(&mut builder, &effective_columns);
    let metadata = BarrageUpdateMetadata::create(
        &mut builder,
        &BarrageUpdateMetadataArgs {
            first_seq: 41,
            last_seq: 43,
            is_snapshot: true,
            effective_viewport: Some(viewport_offset),
            effective_reverse_viewport: true,
            effective_column_set: Some(columns_offset),
            added_rows: Some(added_offset),
            removed_rows: Some(removed_offset),
            shift_data: Some(shifts_offset),
            added_rows_included: Some(included_offset),
            mod_column_nodes: Some(mod_nodes),
            table_size: i64::MAX,
        },
    );
    builder.finish(metadata, None);

    let wrapped = wrap(
        BarrageMessageType::BarrageUpdateMetadata,
        builder.finished_data(),
    );
    let payload = unwrap(&wrapped, BarrageMessageType::BarrageUpdateMetadata);
    let read = flatbuffers::root::<BarrageUpdateMetadata>(payload).expect("valid metadata");

    assert_eq!(read.first_seq(), 41);
    assert_eq!(read.last_seq(), 43);
    assert!(read.is_snapshot());
    assert!(read.effective_reverse_viewport());
    assert_eq!(read.table_size(), i64::MAX);
    assert_eq!(
        read.effective_viewport().expect("viewport").bytes(),
        effective_viewport
    );
    assert_eq!(
        read.effective_column_set().expect("columns").bytes(),
        effective_columns
    );
    assert_eq!(read.added_rows().expect("added").bytes(), added);
    assert_eq!(read.removed_rows().expect("removed").bytes(), removed);
    assert_eq!(read.shift_data().expect("shifts").bytes(), shifts);
    assert_eq!(
        read.added_rows_included().expect("included").bytes(),
        included
    );
    let nodes = read.mod_column_nodes().expect("mod column nodes");
    assert_eq!(nodes.len(), modified.len());
    for (node, expected) in nodes.iter().zip(modified) {
        assert_eq!(
            node.modified_rows().expect("modified rows").bytes(),
            expected
        );
    }
}

#[test]
fn snapshot_request_round_trips_through_the_wrapper() {
    let ticket = *b"s/t";

    let mut builder = FlatBufferBuilder::new();
    let ticket_offset = byte_vector(&mut builder, &ticket);
    let options = BarrageSnapshotOptions::create(
        &mut builder,
        &BarrageSnapshotOptionsArgs {
            use_deephaven_nulls: false,
            batch_size: -1,
            max_message_size: 1024,
            preview_list_length_limit: 3,
        },
    );
    let request = BarrageSnapshotRequest::create(
        &mut builder,
        &BarrageSnapshotRequestArgs {
            ticket: Some(ticket_offset),
            columns: None,
            viewport: None,
            snapshot_options: Some(options),
            reverse_viewport: false,
        },
    );
    builder.finish(request, None);

    let wrapped = wrap(
        BarrageMessageType::BarrageSnapshotRequest,
        builder.finished_data(),
    );
    let payload = unwrap(&wrapped, BarrageMessageType::BarrageSnapshotRequest);
    let read = flatbuffers::root::<BarrageSnapshotRequest>(payload).expect("valid request");

    assert_eq!(read.ticket().expect("ticket").bytes(), ticket);
    assert!(read.columns().is_none());
    assert!(read.viewport().is_none());
    assert!(!read.reverse_viewport());
    let options = read.snapshot_options().expect("options");
    assert!(!options.use_deephaven_nulls());
    assert_eq!(options.batch_size(), -1);
    assert_eq!(options.max_message_size(), 1024);
    assert_eq!(options.preview_list_length_limit(), 3);
}

#[test]
fn malformed_bytes_are_rejected_not_read() {
    assert!(flatbuffers::root::<BarrageMessageWrapper>(&[]).is_err());
    assert!(flatbuffers::root::<BarrageMessageWrapper>(&[0xff; 16]).is_err());

    // A valid wrapper, truncated.
    let wrapped = wrap(BarrageMessageType::None, &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert!(flatbuffers::root::<BarrageMessageWrapper>(&wrapped).is_ok());
    assert!(flatbuffers::root::<BarrageMessageWrapper>(&wrapped[..wrapped.len() - 6]).is_err());
}

/// The tables of the vendored schema and their non-deprecated fields, read from its text.
fn schema_tables(schema: &str) -> Vec<(String, Vec<String>)> {
    let mut tables = Vec::new();
    let mut current: Option<(String, Vec<String>)> = None;
    for line in schema.lines() {
        let line = line.split("//").next().unwrap_or("").trim();
        if let Some(rest) = line.strip_prefix("table ") {
            assert!(current.is_none(), "nested table");
            let name = rest.trim_end_matches('{').trim().to_owned();
            current = Some((name, Vec::new()));
        } else if line == "}" {
            if let Some(table) = current.take() {
                tables.push(table);
            }
        } else if let Some((_, fields)) = current.as_mut() {
            if let Some((name, _)) = line.split_once(':') {
                if !line.contains("(deprecated)") {
                    fields.push(name.trim().to_owned());
                }
            }
        }
    }
    tables
}

#[test]
fn generated_code_covers_every_table_and_field_of_the_schema() {
    let schema = include_str!("../proto/barrage/Barrage.fbs");
    let generated = include_str!("../src/barrage/Barrage_generated.rs");

    let tables = schema_tables(schema);
    let names: Vec<&str> = tables.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "BarrageMessageWrapper",
            "BarrageSubscriptionOptions",
            "BarrageSubscriptionRequest",
            "BarrageSnapshotOptions",
            "BarrageSnapshotRequest",
            "BarragePublicationOptions",
            "BarragePublicationRequest",
            "BarrageModColumnMetadata",
            "BarrageUpdateMetadata",
        ]
    );

    for (table, fields) in &tables {
        // The accessors of one table sit between its struct and its `Args` struct.
        let start = generated
            .find(&format!("pub struct {table}<'a> {{"))
            .unwrap_or_else(|| panic!("no generated struct for table {table}"));
        let end = generated[start..]
            .find(&format!("pub struct {table}Args"))
            .unwrap_or_else(|| panic!("no generated Args for table {table}"));
        let body = &generated[start..start + end];

        assert!(!fields.is_empty(), "table {table} has no fields");
        for field in fields {
            assert!(
                body.contains(&format!("pub fn {field}(&self)")),
                "no generated accessor for {table}.{field}"
            );
        }
        let accessors = body.matches("(&self)").count();
        assert_eq!(accessors, fields.len(), "accessor count for table {table}");
    }
}
