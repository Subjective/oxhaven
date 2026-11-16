//! A Kafka consumer over the wire protocol: the partitions of a topic, their first and last
//! offsets, and the records from a position on.
//!
//! It is what `org.apache.kafka.clients.consumer.KafkaConsumer` does for
//! `io.deephaven.kafka.ingest.KafkaIngester`, which assigns partitions itself and never
//! joins a consumer group. The messages are encoded by the `kafka-protocol` crate; the
//! connections are plain blocking TCP, one per broker, used by the one thread that polls.
//! Not here, and refused by naming what Java does: TLS and SASL, compressed record batches,
//! the offsets a consumer group has committed, and recovery when a partition changes
//! leader. `docs/KAFKA.md`, "The client library".

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;

use crate::{not_ported, InitialOffset, KafkaError, Record, RecordSource};

/// The versions asked of every broker. Brokers from 2.4 on answer all three.
const METADATA_VERSION: i16 = 8;
const LIST_OFFSETS_VERSION: i16 = 5;
const FETCH_VERSION: i16 = 11;

/// How long a broker may hold a fetch that has nothing to return, as the half second
/// `KafkaIngester` passes to `KafkaConsumer#poll`.
const MAX_WAIT_MS: i32 = 500;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(30);
/// `fetch.max.bytes` and `max.partition.fetch.bytes` of a Java consumer left at its defaults.
const FETCH_MAX_BYTES: i32 = 52_428_800;
const PARTITION_MAX_BYTES: i32 = 1_048_576;

fn io_error(what: &str, error: impl std::fmt::Display) -> KafkaError {
    KafkaError::Broker(format!("{what}: {error}"))
}

/// The name of a broker error code, for the ones a consumer of one topic meets.
fn error_name(code: i16) -> String {
    let name = match code {
        1 => "OFFSET_OUT_OF_RANGE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        5 => "LEADER_NOT_AVAILABLE",
        6 => "NOT_LEADER_OR_FOLLOWER",
        29 => "TOPIC_AUTHORIZATION_FAILED",
        35 => "UNSUPPORTED_VERSION",
        _ => return format!("error code {code}"),
    };
    format!("{name} ({code})")
}

struct Connection {
    stream: TcpStream,
    correlation: i32,
}

impl Connection {
    fn open(address: &str) -> Result<Self, KafkaError> {
        let mut last = None;
        let addresses = address
            .to_socket_addrs()
            .map_err(|e| io_error(&format!("cannot resolve {address}"), e))?;
        for socket in addresses {
            match TcpStream::connect_timeout(&socket, SOCKET_TIMEOUT) {
                Ok(stream) => {
                    stream
                        .set_read_timeout(Some(SOCKET_TIMEOUT))
                        .and_then(|()| stream.set_write_timeout(Some(SOCKET_TIMEOUT)))
                        .and_then(|()| stream.set_nodelay(true))
                        .map_err(|e| io_error("cannot configure the connection", e))?;
                    return Ok(Self {
                        stream,
                        correlation: 0,
                    });
                }
                Err(error) => last = Some(error),
            }
        }
        Err(match last {
            Some(error) => io_error(&format!("cannot connect to {address}"), error),
            None => KafkaError::Broker(format!("{address} has no address")),
        })
    }

    /// Sends one request and reads its response.
    fn call<Q: Encodable, P: Decodable>(
        &mut self,
        key: ApiKey,
        version: i16,
        request: &Q,
    ) -> Result<P, KafkaError> {
        self.correlation = self.correlation.wrapping_add(1);
        let header = RequestHeader::default()
            .with_request_api_key(key as i16)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation)
            .with_client_id(Some(StrBytes::from_static_str("oxhaven")));
        let encode = |e| io_error("cannot encode a request", e);
        let mut body = BytesMut::new();
        header
            .encode(&mut body, key.request_header_version(version))
            .map_err(encode)?;
        request.encode(&mut body, version).map_err(encode)?;
        let length = i32::try_from(body.len()).map_err(|e| io_error("request too large", e))?;
        let send = |e| io_error("cannot send a request", e);
        self.stream.write_all(&length.to_be_bytes()).map_err(send)?;
        self.stream.write_all(&body).map_err(send)?;

        let receive = |e| io_error("cannot read a response", e);
        let mut length = [0u8; 4];
        self.stream.read_exact(&mut length).map_err(receive)?;
        let length = usize::try_from(i32::from_be_bytes(length))
            .map_err(|e| io_error("a response has a negative length", e))?;
        let mut response = vec![0u8; length];
        self.stream.read_exact(&mut response).map_err(receive)?;
        let mut response = Bytes::from(response);
        let decode = |e| io_error("cannot decode a response", e);
        let header = ResponseHeader::decode(&mut response, key.response_header_version(version))
            .map_err(decode)?;
        if header.correlation_id != self.correlation {
            return Err(KafkaError::Broker(format!(
                "a response answers request {}, not {}",
                header.correlation_id, self.correlation
            )));
        }
        P::decode(&mut response, version).map_err(decode)
    }
}

struct Partition {
    leader: i32,
    /// The offset of the next record to return.
    position: i64,
}

/// The records of some partitions of one topic, from a position in each.
pub struct WireSource {
    topic: TopicName,
    brokers: BTreeMap<i32, String>,
    connections: BTreeMap<i32, Connection>,
    partitions: BTreeMap<i32, Partition>,
}

impl WireSource {
    /// Connects to the first of `bootstrap` (`host:port`, comma separated) that answers,
    /// finds the partitions of `topic` that `wanted` accepts and positions each where
    /// `initial` says.
    ///
    /// A topic the broker does not have has no partitions, and stays without: Java looks
    /// once. With `create_topic` the broker is asked to create it, as a Java consumer asks
    /// unless `allow.auto.create.topics` is false; the broker decides.
    ///
    /// Behavior: io.deephaven.kafka.ingest.KafkaIngester (the constructor):
    /// `KafkaConsumer#partitionsFor`, `#assign`, `#seekToBeginning`, `#seekToEnd`, `#seek`
    pub fn connect(
        bootstrap: &str,
        topic: &str,
        create_topic: bool,
        wanted: &dyn Fn(i32) -> bool,
        initial: &dyn Fn(i32) -> InitialOffset,
    ) -> Result<Self, KafkaError> {
        let mut first = None;
        let mut failures = Vec::new();
        for address in bootstrap
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
        {
            match Connection::open(address) {
                Ok(connection) => {
                    first = Some(connection);
                    break;
                }
                Err(error) => failures.push(error.to_string()),
            }
        }
        let Some(mut first) = first else {
            return Err(KafkaError::Broker(format!(
                "no broker of bootstrap.servers={bootstrap:?} answers: {}",
                failures.join("; ")
            )));
        };

        let name = TopicName(StrBytes::from_string(topic.to_owned()));
        let request = MetadataRequest::default()
            .with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(name.clone()))
            ]))
            .with_allow_auto_topic_creation(create_topic);
        let metadata: MetadataResponse =
            first.call(ApiKey::Metadata, METADATA_VERSION, &request)?;
        let brokers: BTreeMap<i32, String> = metadata
            .brokers
            .iter()
            .map(|b| (b.node_id.0, format!("{}:{}", b.host.as_str(), b.port)))
            .collect();
        let mut partitions = BTreeMap::new();
        for described in &metadata.topics {
            // Behavior: KafkaConsumer#partitionsFor answers a topic the broker does not
            // have, or is only now creating, with no partitions.
            if ![0, 3, 5].contains(&described.error_code) {
                return Err(KafkaError::Broker(format!(
                    "topic {topic}: {}",
                    error_name(described.error_code)
                )));
            }
            for partition in &described.partitions {
                if !wanted(partition.partition_index) {
                    continue;
                }
                if partition.error_code != 0 {
                    return Err(KafkaError::Broker(format!(
                        "topic {topic} partition {}: {}",
                        partition.partition_index,
                        error_name(partition.error_code)
                    )));
                }
                partitions.insert(
                    partition.partition_index,
                    Partition {
                        leader: partition.leader_id.0,
                        position: 0,
                    },
                );
            }
        }
        drop(first);

        let mut source = Self {
            topic: name,
            brokers,
            connections: BTreeMap::new(),
            partitions,
        };
        let numbers: Vec<i32> = source.partitions.keys().copied().collect();
        for number in numbers {
            let position = match initial(number) {
                InitialOffset::Beginning => source.edge(number, -2)?,
                InitialOffset::End => source.edge(number, -1)?,
                InitialOffset::At(offset) => offset,
                InitialOffset::DontSeek => {
                    return Err(not_ported(
                        "org.apache.kafka.clients.consumer.KafkaConsumer#position",
                        "the offset a consumer group committed, or auto.offset.reset \
                         (DONT_SEEK, the default of `offsets`)",
                    ))
                }
            };
            if let Some(partition) = source.partitions.get_mut(&number) {
                partition.position = position;
            }
        }
        Ok(source)
    }

    /// The partitions being read, in order.
    pub fn partitions(&self) -> Vec<i32> {
        self.partitions.keys().copied().collect()
    }

    fn connection(&mut self, broker: i32) -> Result<&mut Connection, KafkaError> {
        if !self.connections.contains_key(&broker) {
            let address = self
                .brokers
                .get(&broker)
                .ok_or_else(|| KafkaError::Broker(format!("broker {broker} is not known")))?;
            self.connections.insert(broker, Connection::open(address)?);
        }
        Ok(self.connections.get_mut(&broker).expect("just inserted"))
    }

    /// The first offset of a partition (`timestamp` -2) or the one after its last (-1).
    fn edge(&mut self, number: i32, timestamp: i64) -> Result<i64, KafkaError> {
        let leader = self.partitions[&number].leader;
        let request = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![ListOffsetsTopic::default()
                .with_name(self.topic.clone())
                .with_partitions(vec![ListOffsetsPartition::default()
                    .with_partition_index(number)
                    .with_current_leader_epoch(-1)
                    .with_timestamp(timestamp)])]);
        let response: ListOffsetsResponse =
            self.connection(leader)?
                .call(ApiKey::ListOffsets, LIST_OFFSETS_VERSION, &request)?;
        let answer = response
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .find(|partition| partition.partition_index == number)
            .ok_or_else(|| KafkaError::Broker(format!("no offset for partition {number}")))?;
        if answer.error_code != 0 {
            return Err(KafkaError::Broker(format!(
                "offsets of partition {number}: {}",
                error_name(answer.error_code)
            )));
        }
        Ok(answer.offset)
    }
}

/// Decodes the record batches of one partition's fetch response, returns the records from
/// `position` on and the position after them.
///
/// A response may end in part of a batch, which is left for the next fetch, and may start
/// before `position`, because a broker returns whole batches.
///
/// Behavior: org.apache.kafka.clients.consumer.internals.CompletedFetch#fetchRecords
fn decode_records(
    number: i32,
    mut position: i64,
    mut data: Bytes,
    records: &mut Vec<Record>,
) -> Result<i64, KafkaError> {
    // baseOffset (8), batchLength (4), partitionLeaderEpoch (4), magic (1), crc (4),
    // attributes (2), lastOffsetDelta (4)
    const HEADER: usize = 27;
    while data.len() >= 12 {
        let base = i64::from_be_bytes(data[0..8].try_into().expect("8 bytes"));
        let length = i32::from_be_bytes(data[8..12].try_into().expect("4 bytes"));
        let total = 12
            + usize::try_from(length)
                .unwrap_or(usize::MAX)
                .min(usize::MAX - 12);
        if data.len() < total {
            break;
        }
        let mut batch = data.split_to(total);
        if total < HEADER {
            return Err(KafkaError::Broker(format!(
                "partition {number}: a record batch of {total} bytes"
            )));
        }
        let magic = batch[16];
        if magic != 2 {
            return Err(not_ported(
                "org.apache.kafka.common.record.AbstractLegacyRecordBatch",
                &format!("record batches of message format {magic}"),
            ));
        }
        let attributes = i16::from_be_bytes(batch[21..23].try_into().expect("2 bytes"));
        if attributes & 0x7 != 0 {
            return Err(not_ported(
                "org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator",
                &format!(
                    "compressed record batches (compression type {})",
                    attributes & 0x7
                ),
            ));
        }
        let last_delta = i32::from_be_bytes(batch[23..27].try_into().expect("4 bytes"));
        let decoded = RecordBatchDecoder::decode(&mut batch)
            .map_err(|e| io_error(&format!("partition {number}: cannot decode records"), e))?;
        for record in decoded.records {
            if record.control || record.offset < position {
                continue;
            }
            records.push(Record {
                partition: number,
                offset: record.offset,
                timestamp: record.timestamp,
                key: record.key,
                value: record.value,
            });
        }
        position = position.max(base + i64::from(last_delta) + 1);
    }
    Ok(position)
}

impl RecordSource for WireSource {
    /// Behavior: org.apache.kafka.clients.consumer.KafkaConsumer#poll, for the records only
    fn poll(&mut self) -> Result<Vec<Record>, KafkaError> {
        let mut by_leader: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
        for (number, partition) in &self.partitions {
            by_leader.entry(partition.leader).or_default().push(*number);
        }
        if by_leader.is_empty() {
            std::thread::sleep(Duration::from_millis(MAX_WAIT_MS as u64));
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for (leader, numbers) in by_leader {
            let partitions = numbers
                .iter()
                .map(|number| {
                    FetchPartition::default()
                        .with_partition(*number)
                        .with_current_leader_epoch(-1)
                        .with_fetch_offset(self.partitions[number].position)
                        .with_log_start_offset(-1)
                        .with_partition_max_bytes(PARTITION_MAX_BYTES)
                })
                .collect();
            let request = FetchRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_max_wait_ms(MAX_WAIT_MS)
                .with_min_bytes(1)
                .with_max_bytes(FETCH_MAX_BYTES)
                .with_isolation_level(0)
                .with_session_id(0)
                .with_session_epoch(-1)
                .with_topics(vec![FetchTopic::default()
                    .with_topic(self.topic.clone())
                    .with_partitions(partitions)]);
            let response: FetchResponse =
                self.connection(leader)?
                    .call(ApiKey::Fetch, FETCH_VERSION, &request)?;
            if response.error_code != 0 {
                return Err(KafkaError::Broker(format!(
                    "fetch: {}",
                    error_name(response.error_code)
                )));
            }
            for partition in response.responses.into_iter().flat_map(|t| t.partitions) {
                let number = partition.partition_index;
                if partition.error_code != 0 {
                    return Err(KafkaError::Broker(format!(
                        "fetch of partition {number}: {}",
                        error_name(partition.error_code)
                    )));
                }
                let Some(state) = self.partitions.get_mut(&number) else {
                    continue;
                };
                if let Some(data) = partition.records {
                    state.position = decode_records(number, state.position, data, &mut records)?;
                }
            }
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::{
        Compression, Record as WireRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    fn wire_record(offset: i64, value: &str) -> WireRecord {
        WireRecord {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: 1000 + offset,
            key: None,
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Default::default(),
        }
    }

    fn batch(records: &[WireRecord]) -> BytesMut {
        let mut out = BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        RecordBatchEncoder::encode(&mut out, records, &options).unwrap();
        out
    }

    #[test]
    fn records_before_the_position_and_a_partial_last_batch_are_left_out() {
        let mut data = batch(&[wire_record(5, "a"), wire_record(6, "b")]);
        let second = batch(&[wire_record(7, "c")]);
        data.extend_from_slice(&second);
        let third = batch(&[wire_record(8, "d")]);
        data.extend_from_slice(&third[..third.len() - 1]);

        let mut records = Vec::new();
        let position = decode_records(3, 6, data.freeze(), &mut records).unwrap();
        assert_eq!(position, 8);
        let seen: Vec<(i32, i64, i64, &[u8])> = records
            .iter()
            .map(|r| {
                (
                    r.partition,
                    r.offset,
                    r.timestamp,
                    &r.value.as_ref().unwrap()[..],
                )
            })
            .collect();
        assert_eq!(seen, [(3, 6, 1006, &b"b"[..]), (3, 7, 1007, &b"c"[..])]);
    }

    #[test]
    fn control_records_move_the_position_and_are_not_returned() {
        let mut marker = wire_record(4, "commit");
        marker.control = true;
        let data = batch(&[marker]);
        let mut records = Vec::new();
        let position = decode_records(0, 4, data.freeze(), &mut records).unwrap();
        assert_eq!(position, 5);
        assert!(records.is_empty());
    }

    #[test]
    fn a_compressed_batch_is_refused_by_name() {
        let mut data = batch(&[wire_record(0, "a")]);
        data[22] |= 1;
        let error = decode_records(0, 0, data.freeze(), &mut Vec::new()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "not ported yet: org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator: \
             compressed record batches (compression type 1)"
        );
    }
}
