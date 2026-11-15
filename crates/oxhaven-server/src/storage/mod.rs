//! The `StorageService` gRPC service: the files the web IDE keeps on the server, which are
//! its saved layouts and the notebooks of its File Explorer.
//!
//! Java keeps them under a storage directory that it creates with two empty directories in
//! it. A server started with a storage directory ([`StorageRoot`], `--storage-root`) does
//! the same over that directory. A client names every path, so [`root`] is where the checks
//! are that keep a call inside the directory.
//!
//! A server without a storage directory stores nothing. It answers `ListItems` as Java does
//! for a storage before anything was saved: the root holds the directories `/layouts` and
//! `/notebooks`, and they hold nothing. The calls that read or change files are refused
//! with `UNIMPLEMENTED` and their name.

mod etag;
mod root;
#[cfg(test)]
mod root_tests;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use oxhaven_proto::grpc::storage_service_server::StorageService;
use oxhaven_proto::grpc::{
    CreateDirectoryRequest, CreateDirectoryResponse, DeleteItemRequest, DeleteItemResponse,
    FetchFileRequest, FetchFileResponse, ItemInfo, ItemType, ListItemsRequest, ListItemsResponse,
    MoveItemRequest, MoveItemResponse, SaveFileRequest, SaveFileResponse,
};
use tonic::{Request, Response, Status};

use crate::session::current_session;
use crate::table::support::internal_status;
pub use root::StorageRoot;
use root::DIRECTORIES;

/// The status of a storage call on a server without a storage directory.
fn unimplemented(method: &str) -> Status {
    Status::unimplemented(format!(
        "StorageService/{method} is not implemented: this server stores no files"
    ))
}

/// Runs work on the file system off the threads that serve calls.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| internal_status(format_args!("a storage call did not finish: {e}")))?
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

/// The directory a path names on a server without a storage directory, as its names from
/// the root down. One leading `/` is dropped; empty names and `.` are skipped; `..` takes
/// the last name away again, whether or not a directory of that name exists.
///
/// A path that leaves the root is `INVALID_ARGUMENT`. Java also accepts a path that leaves
/// the root and comes back through the root's own name (`/../storage/layouts`), and so does
/// a server with a storage directory; the storage of a server without one has no name
/// outside itself, so such a path is refused there.
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
pub struct StorageGrpc {
    root: Option<Arc<StorageRoot>>,
}

impl StorageGrpc {
    /// The service over `root`, or that of a server that stores nothing.
    pub fn new(root: Option<Arc<StorageRoot>>) -> Self {
        Self { root }
    }

    /// The storage directory, or the refusal of `method` on a server that has none.
    fn root(&self, method: &str) -> Result<Arc<StorageRoot>, Status> {
        self.root.clone().ok_or_else(|| unimplemented(method))
    }
}

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
        let matches = move |name: &str| {
            pattern
                .as_ref()
                .is_none_or(|pattern| glob_matches(pattern, &name.chars().collect::<Vec<_>>()))
        };
        if let Some(root) = self.root.clone() {
            let (canonical_path, items) =
                blocking(move || root.list(&request.path, matches)).await?;
            return Ok(Response::new(ListItemsResponse {
                items,
                canonical_path,
            }));
        }
        let directory = resolve(&request.path)?;
        let names: &[&str] = match directory[..] {
            [] => &DIRECTORIES,
            [name] if DIRECTORIES.contains(&name) => &[],
            _ => return Err(Status::failed_precondition("Directory does not exist")),
        };
        let items = names
            .iter()
            .filter(|name| matches(name))
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

    /// Behavior: `FilesystemStorageServiceGrpcImpl#fetchFile`
    async fn fetch_file(
        &self,
        request: Request<FetchFileRequest>,
    ) -> Result<Response<FetchFileResponse>, Status> {
        current_session(&request)?;
        let root = self.root("FetchFile")?;
        let request = request.into_inner();
        let (contents, etag) = blocking(move || root.fetch(&request.path)).await?;
        // a client that has the contents of this etag is not sent them again
        let unchanged = request.etag.as_deref() == Some(etag.as_str());
        Ok(Response::new(FetchFileResponse {
            contents: if unchanged { Vec::new() } else { contents }.into(),
            etag: Some(etag),
        }))
    }

    /// Behavior: `FilesystemStorageServiceGrpcImpl#saveFile`
    async fn save_file(
        &self,
        request: Request<SaveFileRequest>,
    ) -> Result<Response<SaveFileResponse>, Status> {
        current_session(&request)?;
        let root = self.root("SaveFile")?;
        let request = request.into_inner();
        let etag =
            blocking(move || root.save(&request.path, &request.contents, request.allow_overwrite))
                .await?;
        Ok(Response::new(SaveFileResponse { etag: Some(etag) }))
    }

    /// Behavior: `FilesystemStorageServiceGrpcImpl#moveItem`
    async fn move_item(
        &self,
        request: Request<MoveItemRequest>,
    ) -> Result<Response<MoveItemResponse>, Status> {
        current_session(&request)?;
        let root = self.root("MoveItem")?;
        let request = request.into_inner();
        blocking(move || {
            root.move_item(
                &request.old_path,
                &request.new_path,
                request.allow_overwrite,
            )
        })
        .await?;
        Ok(Response::new(MoveItemResponse {}))
    }

    /// Behavior: `FilesystemStorageServiceGrpcImpl#createDirectory`
    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequest>,
    ) -> Result<Response<CreateDirectoryResponse>, Status> {
        current_session(&request)?;
        let root = self.root("CreateDirectory")?;
        let request = request.into_inner();
        blocking(move || root.create_directory(&request.path)).await?;
        Ok(Response::new(CreateDirectoryResponse {}))
    }

    /// Behavior: `FilesystemStorageServiceGrpcImpl#deleteItem`
    async fn delete_item(
        &self,
        request: Request<DeleteItemRequest>,
    ) -> Result<Response<DeleteItemResponse>, Status> {
        current_session(&request)?;
        let root = self.root("DeleteItem")?;
        let request = request.into_inner();
        blocking(move || root.delete(&request.path)).await?;
        Ok(Response::new(DeleteItemResponse {}))
    }
}
