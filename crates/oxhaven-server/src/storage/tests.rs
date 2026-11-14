//! What Java answers to `ListItems` is replayed in `crate::ide` (the `storage_*` cases).
//! The tests here cover the pieces of it and the calls that are refused.

use oxhaven_proto::grpc::storage_service_client::StorageServiceClient;
use oxhaven_proto::grpc::{
    CreateDirectoryRequest, DeleteItemRequest, FetchFileRequest, MoveItemRequest, SaveFileRequest,
};
use tonic::Code;

use super::*;
use crate::test_support::{with_auth, TestServer};

fn matches(glob: &str, name: &str) -> bool {
    let pattern = parse_glob(glob).unwrap();
    glob_matches(&pattern, &name.chars().collect::<Vec<_>>())
}

#[test]
fn a_glob_has_to_match_the_whole_name() {
    assert!(matches("*", "layouts"));
    assert!(matches("*", ""));
    assert!(matches("lay*", "layouts"));
    assert!(matches("*outs", "layouts"));
    assert!(matches("l*y*s", "layouts"));
    assert!(matches("l?youts", "layouts"));
    assert!(matches("layouts", "layouts"));
    assert!(matches("*.json", "a.json"));
    assert!(matches("*.json", ".json"));
    assert!(!matches("", "layouts"));
    assert!(matches("", ""));
    assert!(!matches("layout", "layouts"));
    assert!(!matches("ayouts", "layouts"));
    assert!(!matches("layouts?", "layouts"));
    assert!(!matches("?", ""));
    // a character of regular expressions stands for itself
    assert!(!matches("l.youts", "layouts"));
    assert!(matches("a.b", "a.b"));
    // `?` is one character, not one byte
    assert!(matches("caf?", "caf\u{e9}"));
}

#[test]
fn a_backslash_makes_the_next_character_literal() {
    assert!(matches("\\layouts", "layouts"));
    assert!(matches("a\\*", "a*"));
    assert!(!matches("a\\*", "ab"));
    assert!(matches("a\\?b", "a?b"));
    assert!(matches("\\[x\\]", "[x]"));
    assert!(matches("a\\\\b", "a\\b"));
}

#[test]
fn globs_java_refuses_and_globs_that_are_not_ported() {
    let refused = |glob: &str| {
        let status = parse_glob(glob).unwrap_err();
        (status.code(), status.message().to_string())
    };
    assert_eq!(
        refused("a**"),
        (
            Code::InvalidArgument,
            "Bad glob, only single `*`s are supported".to_string()
        )
    );
    assert_eq!(
        refused("a/b"),
        (
            Code::InvalidArgument,
            "Bad glob, only the same directory can be checked".to_string()
        )
    );
    // both problems: Java looks for `**` first
    assert_eq!(
        refused("**/x").1,
        "Bad glob, only single `*`s are supported"
    );
    for glob in ["[ab]", "x{a,b}", "trailing\\"] {
        let (code, message) = refused(glob);
        assert_eq!(code, Code::Unimplemented, "{glob}");
        assert!(
            message.contains("sun.nio.fs.Globs#toUnixRegexPattern"),
            "{message}"
        );
    }
}

fn names(path: &str) -> Result<Vec<&str>, String> {
    resolve(path).map_err(|status| status.message().to_string())
}

#[test]
fn a_path_is_normalized_without_looking_at_what_exists() {
    assert_eq!(names(""), Ok(vec![]));
    assert_eq!(names("/"), Ok(vec![]));
    assert_eq!(names("layouts"), Ok(vec!["layouts"]));
    assert_eq!(names("/layouts//x/./"), Ok(vec!["layouts", "x"]));
    assert_eq!(names("/nope/../layouts"), Ok(vec!["layouts"]));
    assert_eq!(names("/a/b/../.."), Ok(vec![]));
    assert_eq!(names("/.."), Err("Invalid path: ..".to_string()));
    assert_eq!(
        names("/a/../../a"),
        Err("Invalid path: a/../../a".to_string())
    );
    // only one leading slash is dropped
    assert_eq!(
        names("//layouts"),
        Err("Invalid path: /layouts".to_string())
    );
    let nul = resolve("/lay\0outs").unwrap_err();
    assert_eq!(nul.code(), Code::InvalidArgument);
    assert!(nul.message().starts_with("Details Logged w/ID '"));
}

#[tokio::test]
async fn calls_that_read_or_change_files_are_unimplemented_by_name() {
    let server = TestServer::start().await;
    let bearer = server.login().await;
    let mut storage = StorageServiceClient::new(server.channel.clone());
    let refused = [
        (
            "FetchFile",
            storage
                .fetch_file(with_auth(FetchFileRequest::default(), &bearer))
                .await
                .unwrap_err(),
        ),
        (
            "SaveFile",
            storage
                .save_file(with_auth(SaveFileRequest::default(), &bearer))
                .await
                .unwrap_err(),
        ),
        (
            "MoveItem",
            storage
                .move_item(with_auth(MoveItemRequest::default(), &bearer))
                .await
                .unwrap_err(),
        ),
        (
            "CreateDirectory",
            storage
                .create_directory(with_auth(CreateDirectoryRequest::default(), &bearer))
                .await
                .unwrap_err(),
        ),
        (
            "DeleteItem",
            storage
                .delete_item(with_auth(DeleteItemRequest::default(), &bearer))
                .await
                .unwrap_err(),
        ),
    ];
    for (method, status) in refused {
        assert_eq!(status.code(), Code::Unimplemented, "{method}");
        assert_eq!(
            status.message(),
            format!("StorageService/{method} is not implemented: this server stores no files")
        );
    }
}
