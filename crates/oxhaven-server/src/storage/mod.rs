//! The `StorageService` gRPC service of a server that stores nothing.
//!
//! The web IDE asks for its saved layouts when it starts. Java keeps them, and notebooks, as
//! files under a storage directory that it creates with two empty directories in it. This
//! server answers `ListItems` as Java does for that storage before anything was saved: the
//! root holds the directories `/layouts` and `/notebooks`, and they hold nothing. The calls
//! that read or change files are refused with `UNIMPLEMENTED` and their name.

#[cfg(test)]
mod tests;

use oxhaven_proto::grpc::storage_service_server::StorageService;
use oxhaven_proto::grpc::{
    CreateDirectoryRequest, CreateDirectoryResponse, DeleteItemRequest, DeleteItemResponse,
    FetchFileRequest, FetchFileResponse, ItemInfo, ItemType, ListItemsRequest, ListItemsResponse,
    MoveItemRequest, MoveItemResponse, SaveFileRequest, SaveFileResponse,
};
use tonic::{Request, Response, Status};

use crate::session::current_session;
use crate::table::support::internal_status;

/// The directories Java creates in an empty storage: the values of the configuration
/// constants `web.storage.layout.directory` and `web.storage.notebook.directory`, sorted.
///
/// Behavior: `FilesystemStorageServiceGrpcImpl#PRE_CREATE_PATHS`
const DIRECTORIES: [&str; 2] = ["layouts", "notebooks"];

/// The status of a storage call this server does not have.
fn unimplemented(method: &str) -> Status {
    Status::unimplemented(format!(
        "StorageService/{method} is not implemented: this server stores no files"
    ))
}

/// One piece of a file name pattern.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Glob {
    Literal(char),
    /// `?`: any one character.
    One,
    /// `*`: any characters, or none.
    Run,
}

/// Reads the pattern of `ListItemsRequest.filter_glob`: `*`, `?`, `\` before a character
/// that stands for itself, and everything else for itself.
///
/// Java's patterns also have character classes (`[a-c]`) and alternatives (`{a,b}`), and a
/// malformed pattern is `INVALID_ARGUMENT` with the message of Java's regular expression
/// compiler. Those are not ported: a pattern with `[` or `{`, or one that ends in `\`, is
/// `UNIMPLEMENTED`.
///
/// Behavior: `FilesystemStorageServiceGrpcImpl#createPathFilter`,
/// `sun.nio.fs.Globs#toUnixRegexPattern`
fn parse_glob(glob: &str) -> Result<Vec<Glob>, Status> {
    if glob.contains("**") {
        return Err(Status::invalid_argument(
            "Bad glob, only single `*`s are supported",
        ));
    }
    if glob.contains('/') {
        return Err(Status::invalid_argument(
            "Bad glob, only the same directory can be checked",
        ));
    }
    let unported = || {
        Status::unimplemented(format!(
            "StorageService/ListItems: the filter_glob {glob:?} is not supported, only `*`, \
             `?` and `\\` are (unported: sun.nio.fs.Globs#toUnixRegexPattern)"
        ))
    };
    let mut pattern = Vec::new();
    let mut characters = glob.chars();
    while let Some(character) = characters.next() {
        pattern.push(match character {
            '*' => Glob::Run,
            '?' => Glob::One,
            '\\' => Glob::Literal(characters.next().ok_or_else(unported)?),
            '[' | '{' => return Err(unported()),
            other => Glob::Literal(other),
        });
    }
    Ok(pattern)
}

/// Whether the whole of `name` matches `pattern`.
fn glob_matches(pattern: &[Glob], name: &[char]) -> bool {
    match pattern.split_first() {
        None => name.is_empty(),
        Some((Glob::Run, rest)) => (0..=name.len()).any(|skip| glob_matches(rest, &name[skip..])),
        Some((Glob::One, rest)) => !name.is_empty() && glob_matches(rest, &name[1..]),
        Some((Glob::Literal(expected), rest)) => {
            name.first() == Some(expected) && glob_matches(rest, &name[1..])
        }
    }
}

/// The directory a path names, as its names from the root down. One leading `/` is dropped;
/// empty names and `.` are skipped; `..` takes the last name away again, whether or not a
/// directory of that name exists.
///
/// A path that leaves the root is `INVALID_ARGUMENT`. Java also accepts a path that leaves
/// the root and comes back through the root's own name (`/../storage/layouts`); the storage
/// of this server has no name outside itself, so such a path is refused here.
///
/// Behavior: `FilesystemStorageServiceGrpcImpl#resolveOrThrow`, `java.nio.file.Path#normalize`
fn resolve(path: &str) -> Result<Vec<&str>, Status> {
    let path = path.strip_prefix('/').unwrap_or(path);
    let invalid = || Status::invalid_argument(format!("Invalid path: {path}"));
    if path.contains('\0') {
        // Java's file system refuses the character with an exception of its own
        return Err(internal_status(format_args!(
            "Nul character not allowed: {path:?}"
        )));
    }
    if path.starts_with('/') {
        // still absolute: Java resolves it against the file system's root
        return Err(invalid());
    }
    let mut names = Vec::new();
    for name in path.split('/') {
        match name {
            "" | "." => {}
            ".." => {
                names.pop().ok_or_else(invalid)?;
            }
            name => names.push(name),
        }
    }
    Ok(names)
}

/// Behavior: `io.deephaven.server.notebook.FilesystemStorageServiceGrpcImpl`
pub struct StorageGrpc;

#[tonic::async_trait]
impl StorageService for StorageGrpc {
    /// Behavior: `FilesystemStorageServiceGrpcImpl#listItems`
    async fn list_items(
        &self,
        request: Request<ListItemsRequest>,
    ) -> Result<Response<ListItemsResponse>, Status> {
        current_session(&request)?;
        let request = request.into_inner();
        let pattern = request.filter_glob.as_deref().map(parse_glob).transpose()?;
        let directory = resolve(&request.path)?;
        let names: &[&str] = match directory[..] {
            [] => &DIRECTORIES,
            [name] if DIRECTORIES.contains(&name) => &[],
            _ => return Err(Status::failed_precondition("Directory does not exist")),
        };
        let items = names
            .iter()
            .filter(|name| {
                pattern
                    .as_ref()
                    .is_none_or(|pattern| glob_matches(pattern, &name.chars().collect::<Vec<_>>()))
            })
            .map(|name| ItemInfo {
                path: format!("/{name}"),
                r#type: ItemType::Directory as i32,
                size: 0,
                etag: None,
            })
            .collect();
        Ok(Response::new(ListItemsResponse {
            items,
            canonical_path: format!("/{}", directory.join("/")),
        }))
    }

    async fn fetch_file(
        &self,
        _request: Request<FetchFileRequest>,
    ) -> Result<Response<FetchFileResponse>, Status> {
        Err(unimplemented("FetchFile"))
    }

    async fn save_file(
        &self,
        _request: Request<SaveFileRequest>,
    ) -> Result<Response<SaveFileResponse>, Status> {
        Err(unimplemented("SaveFile"))
    }

    async fn move_item(
        &self,
        _request: Request<MoveItemRequest>,
    ) -> Result<Response<MoveItemResponse>, Status> {
        Err(unimplemented("MoveItem"))
    }

    async fn create_directory(
        &self,
        _request: Request<CreateDirectoryRequest>,
    ) -> Result<Response<CreateDirectoryResponse>, Status> {
        Err(unimplemented("CreateDirectory"))
    }

    async fn delete_item(
        &self,
        _request: Request<DeleteItemRequest>,
    ) -> Result<Response<DeleteItemResponse>, Status> {
        Err(unimplemented("DeleteItem"))
    }
}
