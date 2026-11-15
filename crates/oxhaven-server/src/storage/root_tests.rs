//! The storage service over a real directory.
//!
//! `fixtures/java.txt` holds what the Java reference answered to a sequence of calls over a
//! new storage directory (`fixtures/capture.py` records it). [`java_recording`] sends the
//! same sequence to this server over a directory of the same shape and compares every
//! answer. The cases of [`DIFFERENCES`] are those where this server does not follow Java:
//! a symbolic link in the storage that points out of it.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use oxhaven_proto::grpc::storage_service_client::StorageServiceClient;
use oxhaven_proto::grpc::{
    CreateDirectoryRequest, DeleteItemRequest, FetchFileRequest, ItemInfo, ItemType,
    ListItemsRequest, ListItemsResponse, MoveItemRequest, SaveFileRequest,
};
use prost::Message;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use super::etag::etag;
use super::StorageRoot;
use crate::test_support::{with_auth, TestServer};

const RECORDING: &str = include_str!("fixtures/java.txt");

/// A directory for one test, with the storage in `storage` and, next to it, `outside`:
/// what a client must not reach. Removed when dropped.
struct Sandbox {
    base: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!("oxhaven-storage-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(base.join("outside")).unwrap();
        Self { base }
    }

    fn storage(&self) -> PathBuf {
        self.base.join("storage")
    }

    fn outside(&self) -> PathBuf {
        self.base.join("outside")
    }

    /// The names in a directory, sorted.
    fn names(directory: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.base);
    }
}

async fn serve(sandbox: &Sandbox) -> (TestServer, String, StorageServiceClient<Channel>) {
    let root = Arc::new(StorageRoot::open(&sandbox.storage()).unwrap());
    let server = TestServer::start_with(|state| state.storage = Some(root)).await;
    let bearer = server.login().await;
    let client = StorageServiceClient::new(server.channel.clone());
    (server, bearer, client)
}

/// One recorded call and what Java answered.
#[derive(Debug, Default)]
struct Case {
    name: String,
    /// Steps in the file system that come before the call: `write`, `mkdir`, `symlink`.
    fs: Vec<String>,
    method: String,
    authenticated: bool,
    request: Vec<u8>,
    /// `OK` or `<CODE>: <message>`.
    status: String,
    message: Option<Vec<u8>>,
}

fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex length");
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

fn recorded() -> Vec<Case> {
    let mut cases: Vec<Case> = Vec::new();
    for line in RECORDING.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix("== ") {
            cases.push(Case {
                name: name.to_string(),
                authenticated: true,
                ..Case::default()
            });
            continue;
        }
        let case = cases.last_mut().expect("a line before the first case");
        let (key, value) = line.split_once(':').expect("key: value");
        let value = value.strip_prefix(' ').unwrap_or(value);
        match key {
            "fs" => case.fs.push(value.to_string()),
            "call" => case.method = value.strip_prefix("StorageService/").unwrap().to_string(),
            "auth" => case.authenticated = value != "none",
            "request" => case.request = unhex(value),
            // no recorded message has a line break or a backslash escape in it
            "status" => case.status = value.to_string(),
            "message" => case.message = Some(unhex(value)),
            other => panic!("unknown line {other} in case {}", case.name),
        }
    }
    assert!(cases.len() > 100, "the recording is incomplete");
    cases
}

/// Does a recorded step in the file system below `base`.
fn apply(base: &Path, step: &str) {
    let mut words = step.splitn(3, ' ');
    let (kind, path) = (words.next().unwrap(), base.join(words.next().unwrap()));
    let argument = words.next().unwrap_or("");
    match kind {
        "write" => fs::write(path, unhex(argument)).unwrap(),
        "mkdir" => fs::create_dir(path).unwrap(),
        "symlink" => std::os::unix::fs::symlink(argument, path).unwrap(),
        other => panic!("unknown step {other}"),
    }
}

fn status_line(status: &Status) -> String {
    let code = match status.code() {
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Unauthenticated => "UNAUTHENTICATED",
        Code::Unimplemented => "UNIMPLEMENTED",
        other => panic!("a status the recording does not have: {other:?}"),
    };
    format!("{code}: {}", status.message())
}

/// Sends a recorded request; the answer as the recording writes one.
async fn send(
    client: &mut StorageServiceClient<Channel>,
    bearer: &str,
    case: &Case,
) -> (String, Option<Vec<u8>>) {
    fn request<T: Message + Default>(case: &Case, bearer: &str) -> Request<T> {
        let message = T::decode(case.request.as_slice()).expect("a recorded request");
        if case.authenticated {
            with_auth(message, bearer)
        } else {
            Request::new(message)
        }
    }
    fn answer<T: Message>(result: Result<tonic::Response<T>, Status>) -> (String, Option<Vec<u8>>) {
        match result {
            Ok(response) => (
                "OK".to_string(),
                Some(response.into_inner().encode_to_vec()),
            ),
            Err(status) => (status_line(&status), None),
        }
    }
    match case.method.as_str() {
        "ListItems" => answer(client.list_items(request(case, bearer)).await),
        "FetchFile" => answer(client.fetch_file(request(case, bearer)).await),
        "SaveFile" => answer(client.save_file(request(case, bearer)).await),
        "MoveItem" => answer(client.move_item(request(case, bearer)).await),
        "CreateDirectory" => answer(client.create_directory(request(case, bearer)).await),
        "DeleteItem" => answer(client.delete_item(request(case, bearer)).await),
        other => panic!("unknown method {other}"),
    }
}

/// The recorded cases this server answers otherwise, with its status. All of them go
/// through a link that points out of the storage, which Java follows.
const DIFFERENCES: &[(&str, &str)] = &[
    (
        "link_fetch_file_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_file_out",
    ),
    (
        "link_list_dir_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_dir_out",
    ),
    (
        "link_save_through_dir_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_dir_out/planted.txt",
    ),
    (
        "link_save_onto_file_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_file_out",
    ),
    (
        "link_save_onto_file_out_no_overwrite",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_file_out",
    ),
    (
        "link_mkdir_through_dir_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_dir_out/made",
    ),
    (
        "link_move_into_dir_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_dir_out/a.py",
    ),
    (
        "link_move_out_of_dir_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_dir_out/planted.txt",
    ),
    (
        "link_delete_through_dir_out",
        "INVALID_ARGUMENT: Invalid path: notebooks/link_dir_out/made",
    ),
];

/// The links of the recording that point out of the storage.
const LINKS_OUT: [&str; 2] = ["/notebooks/link_file_out", "/notebooks/link_dir_out"];
/// The file Java moved into the storage through such a link.
const MOVED_IN_BY_JAVA: &str = "/notebooks/taken.txt";

/// A listing as this server has to give it for the one Java gave: sorted, without what
/// only Java could bring in, and with nothing told about what a link out points to.
fn expected_listing(java: &[u8]) -> ListItemsResponse {
    let mut listing = ListItemsResponse::decode(java).unwrap();
    listing.items.retain(|item| item.path != MOVED_IN_BY_JAVA);
    for item in &mut listing.items {
        if LINKS_OUT.contains(&item.path.as_str()) {
            *item = ItemInfo {
                path: item.path.clone(),
                r#type: ItemType::File as i32,
                size: 0,
                etag: None,
            };
        }
    }
    listing.items.sort_by(|a, b| a.path.cmp(&b.path));
    listing
}

#[tokio::test]
async fn java_recording() {
    let sandbox = Sandbox::new();
    let (_server, bearer, mut client) = serve(&sandbox).await;
    let differences: BTreeMap<&str, &str> = DIFFERENCES.iter().copied().collect();
    let cases = recorded();
    for name in differences.keys() {
        assert!(
            cases.iter().any(|case| case.name == *name),
            "{name} is not recorded"
        );
    }
    let mut failures = Vec::new();
    for case in &cases {
        for step in &case.fs {
            apply(&sandbox.base, step);
        }
        let (status, message) = send(&mut client, &bearer, case).await;
        let expected_status = differences
            .get(case.name.as_str())
            .copied()
            .unwrap_or(&case.status);
        // an error Java has no message for carries an id that is new every time
        let logged = "INVALID_ARGUMENT: Details Logged w/ID '";
        let same_status = if expected_status.starts_with(logged) {
            status.starts_with(logged)
        } else {
            status == expected_status
        };
        if !same_status {
            failures.push(format!(
                "{}: status {status:?}, expected {expected_status:?}",
                case.name
            ));
            continue;
        }
        if status != "OK" {
            continue;
        }
        let (message, java) = (message.unwrap(), case.message.as_ref().unwrap());
        if case.method == "ListItems" {
            let listing = ListItemsResponse::decode(message.as_slice()).unwrap();
            let expected = expected_listing(java);
            if listing != expected {
                failures.push(format!(
                    "{}: listed {listing:?}, expected {expected:?}",
                    case.name
                ));
            }
        } else if &message != java {
            failures.push(format!(
                "{}: answered {message:02x?}, expected {java:02x?}",
                case.name
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    // Nothing outside the storage was read into it, written, created or removed.
    assert_eq!(Sandbox::names(&sandbox.outside()), ["dir", "secret.txt"]);
    assert_eq!(
        fs::read(sandbox.outside().join("secret.txt")).unwrap(),
        b"secret\n"
    );
    assert!(Sandbox::names(&sandbox.outside().join("dir")).is_empty());
}

#[test]
fn etags_are_those_of_the_recording() {
    assert_eq!(etag(b""), "00000000000000000000000000000000");
    assert_eq!(etag(b"print('a')\n"), "e93de59c9d76a3813551338d68ad75da");
    assert_eq!(etag(b"b = 1\n"), "9c12676c44cf5632a57b4653a80ea7de");
    assert_eq!(etag(b"{}"), "d2c2517805206e46be176699782a8820");
    // longer than one block of 16 bytes, with a tail of every length
    let bytes: Vec<u8> = (0..=255).collect();
    assert_eq!(etag(&bytes), "b9126fdc13c3991c1ecc34ab7f07d670");
    assert_eq!(
        etag("s = '\u{2603}'\n".as_bytes()),
        "f7b384a5b90b98187624478963f258c5"
    );
}

fn code_of<T>(result: Result<T, Status>) -> Option<(Code, String)> {
    result
        .err()
        .map(|status| (status.code(), status.message().to_string()))
}

fn invalid(path: &str) -> Option<(Code, String)> {
    Some((Code::InvalidArgument, format!("Invalid path: {path}")))
}

#[test]
fn a_new_storage_has_the_directories_of_the_ide() {
    let sandbox = Sandbox::new();
    let root = StorageRoot::open(&sandbox.storage()).unwrap();
    assert_eq!(Sandbox::names(&sandbox.storage()), ["layouts", "notebooks"]);
    // opening it again keeps what is there
    root.save("/layouts/a.json", b"{}", false).unwrap();
    let root = StorageRoot::open(&sandbox.storage()).unwrap();
    assert_eq!(root.fetch("/layouts/a.json").unwrap().0, b"{}");
    // a file where the directory should be cannot be a storage
    let error = StorageRoot::open(&sandbox.outside().join("../storage/layouts/a.json"));
    assert!(error
        .unwrap_err()
        .starts_with("Failed to initialize storage"));
}

#[test]
fn an_absolute_path_is_taken_as_it_is() {
    let sandbox = Sandbox::new();
    let root = StorageRoot::open(&sandbox.storage()).unwrap();
    root.save("/notebooks/a.py", b"a", false).unwrap();
    // the client's path is still absolute after its first `/`: Java resolves it by itself,
    // and it is valid only if it names something in the storage
    let inside = format!("/{}/notebooks/a.py", sandbox.storage().display());
    assert_eq!(root.fetch(&inside).unwrap().0, b"a");
    let secret = sandbox.outside().join("secret.txt");
    fs::write(&secret, "secret").unwrap();
    let outside = format!("/{}", secret.display());
    assert_eq!(code_of(root.fetch(&outside)), invalid(&outside[1..]));
    assert_eq!(code_of(root.delete(&outside)), invalid(&outside[1..]));
    assert_eq!(
        code_of(root.save(&outside, b"x", true)),
        invalid(&outside[1..])
    );
    assert_eq!(fs::read(&secret).unwrap(), b"secret");
    // a directory whose name starts with the storage's name is not in the storage
    let sibling = format!("/{}2/a", sandbox.storage().display());
    assert_eq!(code_of(root.fetch(&sibling)), invalid(&sibling[1..]));
}

#[test]
fn links_are_followed_to_their_end_before_a_call_is_allowed() {
    use std::os::unix::fs::symlink;
    let sandbox = Sandbox::new();
    let root = StorageRoot::open(&sandbox.storage()).unwrap();
    let notebooks = sandbox.storage().join("notebooks");
    let secret = sandbox.outside().join("secret.txt");
    fs::write(&secret, "secret").unwrap();

    // a link to a link that leaves
    symlink("../../outside/secret.txt", notebooks.join("hop2")).unwrap();
    symlink("hop2", notebooks.join("hop1")).unwrap();
    assert_eq!(
        code_of(root.fetch("/notebooks/hop1")),
        invalid("notebooks/hop1")
    );
    assert_eq!(
        code_of(root.save("/notebooks/hop1", b"x", true)),
        invalid("notebooks/hop1")
    );

    // a link with an absolute target
    symlink(&secret, notebooks.join("absolute")).unwrap();
    assert_eq!(
        code_of(root.fetch("/notebooks/absolute")),
        invalid("notebooks/absolute")
    );
    symlink(sandbox.outside(), notebooks.join("absolute_dir")).unwrap();
    assert_eq!(
        code_of(root.list("/notebooks/absolute_dir", |_| true)),
        invalid("notebooks/absolute_dir")
    );
    assert_eq!(
        code_of(root.fetch("/notebooks/absolute_dir/secret.txt")),
        invalid("notebooks/absolute_dir/secret.txt")
    );
    assert_eq!(
        code_of(root.delete("/notebooks/absolute_dir/secret.txt")),
        invalid("notebooks/absolute_dir/secret.txt")
    );
    assert_eq!(
        code_of(root.move_item("/notebooks/absolute_dir/secret.txt", "/notebooks/got", true)),
        invalid("notebooks/absolute_dir/secret.txt")
    );

    // a link whose target does not exist yet: saving would create the target outside
    symlink("../../outside/planted.txt", notebooks.join("plant")).unwrap();
    assert_eq!(
        code_of(root.save("/notebooks/plant", b"x", true)),
        invalid("notebooks/plant")
    );
    // ... and one that stays inside creates its target, as Java does
    symlink("made_here.py", notebooks.join("inside")).unwrap();
    root.save("/notebooks/inside", b"x", true).unwrap();
    assert_eq!(fs::read(notebooks.join("made_here.py")).unwrap(), b"x");

    // a link that leaves and whose path comes back with `..`: the text of the path is in
    // the storage, the file it names is not
    assert_eq!(
        code_of(root.fetch("/notebooks/absolute_dir/../outside/secret.txt")),
        Some((Code::FailedPrecondition, "File does not exist".to_string()))
    );
    assert_eq!(
        code_of(root.save("/notebooks/absolute_dir/dir/../new.txt", b"x", true)),
        invalid("notebooks/absolute_dir/dir/../new.txt")
    );

    // the links themselves can be renamed and deleted: that touches nothing outside
    root.move_item("/notebooks/absolute", "/notebooks/renamed", false)
        .unwrap();
    for link in ["hop1", "hop2", "renamed", "absolute_dir", "plant", "inside"] {
        root.delete(&format!("/notebooks/{link}")).unwrap();
    }
    assert_eq!(Sandbox::names(&notebooks), ["made_here.py"]);
    assert_eq!(Sandbox::names(&sandbox.outside()), ["secret.txt"]);
    assert_eq!(fs::read(&secret).unwrap(), b"secret");
}

#[test]
fn a_storage_named_through_a_link_is_the_directory_the_link_names() {
    use std::os::unix::fs::symlink;
    let sandbox = Sandbox::new();
    fs::create_dir(sandbox.base.join("real")).unwrap();
    symlink("real", sandbox.storage()).unwrap();
    let root = StorageRoot::open(&sandbox.storage()).unwrap();
    root.save("/notebooks/a.py", b"a", false).unwrap();
    assert_eq!(
        fs::read(sandbox.base.join("real/notebooks/a.py")).unwrap(),
        b"a"
    );
    let (canonical, items) = root.list("/notebooks", |_| true).unwrap();
    assert_eq!(canonical, "/notebooks");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].path, "/notebooks/a.py");
    assert_eq!(
        code_of(root.fetch("/../outside/x")),
        invalid("../outside/x")
    );
}

#[tokio::test]
async fn a_notebook_larger_than_the_default_message_limit_is_saved() {
    let sandbox = Sandbox::new();
    let (_server, bearer, mut client) = serve(&sandbox).await;
    // tonic refuses a message over 4 MiB unless told otherwise; Java's limit is 100 MiB
    let contents = vec![b'x'; 5 * 1024 * 1024];
    let saved = client
        .save_file(with_auth(
            SaveFileRequest {
                allow_overwrite: false,
                path: "/notebooks/large.py".to_string(),
                contents: contents.clone().into(),
            },
            &bearer,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(saved.etag, Some(etag(&contents)));
    let mut client = client.max_decoding_message_size(100 * 1024 * 1024);
    let fetched = client
        .fetch_file(with_auth(
            FetchFileRequest {
                path: "/notebooks/large.py".to_string(),
                etag: None,
            },
            &bearer,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.contents.len(), contents.len());
    // the calls of the other kinds, once each over the wire
    client
        .create_directory(with_auth(
            CreateDirectoryRequest {
                path: "/notebooks/d".to_string(),
            },
            &bearer,
        ))
        .await
        .unwrap();
    client
        .move_item(with_auth(
            MoveItemRequest {
                old_path: "/notebooks/large.py".to_string(),
                new_path: "/notebooks/d/l.py".to_string(),
                allow_overwrite: false,
            },
            &bearer,
        ))
        .await
        .unwrap();
    let listed = client
        .list_items(with_auth(
            ListItemsRequest {
                path: "/notebooks/d".to_string(),
                filter_glob: Some("*.py".to_string()),
            },
            &bearer,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].size, contents.len() as i64);
    client
        .delete_item(with_auth(
            DeleteItemRequest {
                path: "/notebooks/d/l.py".to_string(),
            },
            &bearer,
        ))
        .await
        .unwrap();
    assert!(Sandbox::names(&sandbox.storage().join("notebooks/d")).is_empty());
}
