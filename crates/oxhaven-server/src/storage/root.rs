//! The directory the storage service keeps its files in, and the two checks that keep a
//! client inside it.
//!
//! A client names every file. The first check is Java's: the path is put behind the
//! directory and `.` and `..` are taken out of it as text, and what remains has to start
//! with the directory. That stops `..` and absolute paths. It does not stop a symbolic link
//! inside the directory that points out of it: Java follows such a link and reads, writes,
//! creates and deletes where it leads. The second check is this server's own and has no Java
//! counterpart (`PARITY.md`, A8): where the file system would actually go, with links followed, has to be
//! inside the directory too, and a call that would leave it is refused as an invalid path.
//! A client cannot make a link through this service; someone with the directory can.

use std::fs;
use std::io::{self, ErrorKind, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

use oxhaven_proto::grpc::{ItemInfo, ItemType};
use tonic::Status;

use super::etag::etag;
use crate::table::support::internal_status;

/// The directories Java creates in a new storage: the values of the configuration
/// constants `web.storage.layout.directory` and `web.storage.notebook.directory`.
///
/// Behavior: `FilesystemStorageServiceGrpcImpl#PRE_CREATE_PATHS`
pub(super) const DIRECTORIES: [&str; 2] = ["layouts", "notebooks"];

/// How many links in a row are followed from the last name of a path before the path
/// counts as one the file system refuses, as Linux does.
const MAX_LINKS: usize = 40;

/// Whether a call follows a symbolic link that is the last name of its path.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Last {
    /// The call reads or writes what the link points to.
    Followed,
    /// The call acts on the link itself.
    Itself,
}

/// A path as Java's `Path#normalize` leaves it: no `.`, and no `..` that follows a name.
/// `path` is absolute, so a `..` at the top is dropped as the file system does.
fn normalize(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normal.pop();
            }
            other => normal.push(other),
        }
    }
    normal
}

/// The status of a failure of the file system that Java has no message for.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
fn unexpected(what: &str, path: &Path, error: &io::Error) -> Status {
    internal_status(format_args!("{what} {}: {error}", path.display()))
}

/// The storage directory of a server.
///
/// Behavior: `io.deephaven.server.notebook.FilesystemStorageServiceGrpcImpl`
#[derive(Debug)]
pub struct StorageRoot {
    /// The directory as it was named, absolute and normalized: what Java's check compares
    /// with.
    root: PathBuf,
    /// The directory with every symbolic link followed: what this server's check compares
    /// with.
    real: PathBuf,
}

impl StorageRoot {
    /// Makes `directory` the storage: creates it if it is missing, and in it the
    /// directories the web IDE keeps its layouts and its notebooks in.
    ///
    /// Behavior: `FilesystemStorageServiceGrpcImpl#FilesystemStorageServiceGrpcImpl`
    pub fn open(directory: &Path) -> Result<Self, String> {
        let failed = |e: io::Error| format!("Failed to initialize storage: {directory:?}: {e}");
        let root = normalize(&std::path::absolute(directory).map_err(failed)?);
        fs::create_dir_all(&root).map_err(failed)?;
        for name in DIRECTORIES {
            fs::create_dir_all(root.join(name)).map_err(failed)?;
        }
        let real = fs::canonicalize(&root).map_err(failed)?;
        Ok(Self { root, real })
    }

    /// The directory, as it was named.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The file a client's path names: the path behind the storage directory, without one
    /// leading `/`, normalized as text. A path that then is not the directory or below it
    /// is `INVALID_ARGUMENT`.
    ///
    /// Behavior: `FilesystemStorageServiceGrpcImpl#resolveOrThrow`
    fn resolve(&self, incoming: &str) -> Result<Resolved<'_>, Status> {
        let incoming = incoming.strip_prefix('/').unwrap_or(incoming);
        if incoming.contains('\0') {
            // Java's file system refuses the character with an exception of its own
            return Err(internal_status(format_args!(
                "Nul character not allowed: {incoming:?}"
            )));
        }
        let path = normalize(&self.root.join(incoming));
        if !path.starts_with(&self.root) {
            return Err(Status::invalid_argument(format!(
                "Invalid path: {incoming}"
            )));
        }
        Ok(Resolved {
            storage: self,
            incoming: incoming.to_string(),
            path,
        })
    }

    /// The path a client is told for `path`: from the storage directory, with a leading
    /// `/`.
    fn client_path(&self, path: &Path) -> String {
        let below = path.strip_prefix(&self.root).unwrap_or(path);
        format!("/{}", below.to_string_lossy())
    }

    /// Whether `path`, with every symbolic link followed as `last` says, is in the storage.
    /// A path the file system cannot follow is in it: the call then fails on that path by
    /// itself, with the failure Java has.
    fn holds(&self, path: &Path, last: Last) -> bool {
        match last {
            Last::Followed => self.holds_followed(path, 0),
            Last::Itself => match (path.parent(), path == self.root) {
                (Some(parent), false) => fs::canonicalize(parent)
                    .map(|real| real.starts_with(&self.real))
                    .unwrap_or(true),
                _ => true,
            },
        }
    }

    fn holds_followed(&self, path: &Path, links: usize) -> bool {
        if let Ok(real) = fs::canonicalize(path) {
            return real.starts_with(&self.real);
        }
        // The path names nothing. Its last name may still be a link whose target does not
        // exist, and a file would be created where the link points.
        let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
            return true;
        };
        let Ok(real_parent) = fs::canonicalize(parent) else {
            return true;
        };
        match fs::read_link(real_parent.join(name)) {
            Ok(_) if links >= MAX_LINKS => true,
            Ok(target) => self.holds_followed(&real_parent.join(target), links + 1),
            Err(_) => real_parent.starts_with(&self.real),
        }
    }

    /// Behavior: `FilesystemStorageServiceGrpcImpl#listItems`
    pub(super) fn list(
        &self,
        path: &str,
        matches: impl Fn(&str) -> bool,
    ) -> Result<(String, Vec<ItemInfo>), Status> {
        let directory = self.resolve(path)?.confined(Last::Followed)?;
        let missing = || Status::failed_precondition("Directory does not exist");
        let failed = |e: io::Error| match e.kind() {
            ErrorKind::NotFound => missing(),
            _ => unexpected("cannot list", &directory, &e),
        };
        let mut items = Vec::new();
        for entry in fs::read_dir(&directory).map_err(failed)? {
            let entry = entry.map_err(failed)?;
            let name = entry.file_name();
            if !matches(&name.to_string_lossy()) {
                continue;
            }
            let item = directory.join(&name);
            let mut info = ItemInfo {
                path: self.client_path(&item),
                r#type: ItemType::File as i32,
                size: 0,
                etag: None,
            };
            if !self.holds(&item, Last::Followed) {
                // A link that leaves the storage: Java describes what it points to. Here
                // it is listed, so that it can be seen and deleted, as a file of which
                // nothing is known.
                items.push(info);
                continue;
            }
            // as Java, the attributes are those of what a link points to, and a link that
            // points to nothing fails the listing
            let attributes = fs::metadata(&item).map_err(failed)?;
            if attributes.is_dir() {
                info.r#type = ItemType::Directory as i32;
            } else {
                info.size = attributes.len() as i64;
                info.etag = Some(etag(&fs::read(&item).map_err(failed)?));
            }
            items.push(info);
        }
        // Java lists in the order of the file system, which is none
        items.sort_by(|a, b| a.path.cmp(&b.path));
        Ok((self.client_path(&directory), items))
    }

    /// The contents of a file and their etag.
    ///
    /// Behavior: `FilesystemStorageServiceGrpcImpl#fetchFile`
    pub(super) fn fetch(&self, path: &str) -> Result<(Vec<u8>, String), Status> {
        let file = self.resolve(path)?.confined(Last::Followed)?;
        let contents = fs::read(&file).map_err(|e| match e.kind() {
            ErrorKind::NotFound => Status::failed_precondition("File does not exist"),
            _ => unexpected("cannot read", &file, &e),
        })?;
        let etag = etag(&contents);
        Ok((contents, etag))
    }

    /// Writes a file and returns the etag of its contents. With `overwrite` an existing
    /// file is replaced and a missing one created; without, the file must not exist.
    ///
    /// Behavior: `FilesystemStorageServiceGrpcImpl#saveFile`
    pub(super) fn save(
        &self,
        path: &str,
        contents: &[u8],
        overwrite: bool,
    ) -> Result<String, Status> {
        let file = self
            .resolve(path)?
            .not_root("Can't overwrite the root directory")?
            .confined(Last::Followed)?;
        let mut options = fs::OpenOptions::new();
        options.write(true);
        if overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        }
        options
            .open(&file)
            .and_then(|mut open| open.write_all(contents))
            .map_err(|e| match e.kind() {
                ErrorKind::AlreadyExists => Status::failed_precondition("File already exists"),
                ErrorKind::NotFound => Status::failed_precondition("Directory does not exist"),
                _ => unexpected("cannot write", &file, &e),
            })?;
        Ok(etag(contents))
    }

    /// Renames a file, a directory or a link. A target that exists is refused without
    /// `overwrite`; with it the target is deleted first, which an empty directory or
    /// anything that is not a directory can be. Renaming something to itself does nothing.
    ///
    /// Java moves across file systems by copying. That is not ported: such a move is
    /// `UNIMPLEMENTED`.
    ///
    /// Behavior: `FilesystemStorageServiceGrpcImpl#moveItem`, `sun.nio.fs.UnixFileSystem#move`
    pub(super) fn move_item(&self, old: &str, new: &str, overwrite: bool) -> Result<(), Status> {
        let source = self.resolve(old)?;
        let target = self
            .resolve(new)?
            .not_root("Can't overwrite the root directory")?;
        let source = source.confined(Last::Itself)?;
        let target = target.confined(Last::Itself)?;
        let missing = || Status::failed_precondition("File does not exist, cannot rename");
        let moved = fs::symlink_metadata(&source).map_err(|e| match e.kind() {
            ErrorKind::NotFound => missing(),
            _ => unexpected("cannot move", &source, &e),
        })?;
        if let Ok(existing) = fs::symlink_metadata(&target) {
            if existing.dev() == moved.dev() && existing.ino() == moved.ino() {
                return Ok(());
            }
            if !overwrite {
                return Err(Status::failed_precondition(
                    "File already exists, cannot rename to replace",
                ));
            }
            let deleted = if existing.is_dir() {
                fs::remove_dir(&target)
            } else {
                fs::remove_file(&target)
            };
            deleted.map_err(|e| match e.kind() {
                ErrorKind::DirectoryNotEmpty => {
                    Status::failed_precondition("Cannot replace non-empty directory")
                }
                _ => unexpected("cannot replace", &target, &e),
            })?;
        }
        fs::rename(&source, &target).map_err(|e| match e.kind() {
            ErrorKind::NotFound => missing(),
            ErrorKind::CrossesDevices => Status::unimplemented(
                "StorageService/MoveItem: moving to another file system is not supported \
                 (unported: sun.nio.fs.UnixFileSystem#move)",
            ),
            _ => unexpected("cannot move", &source, &e),
        })
    }

    /// Behavior: `FilesystemStorageServiceGrpcImpl#createDirectory`
    pub(super) fn create_directory(&self, path: &str) -> Result<(), Status> {
        let directory = self
            .resolve(path)?
            .not_root("Can't overwrite the root directory")?
            .confined(Last::Itself)?;
        fs::create_dir(&directory).map_err(|e| match e.kind() {
            ErrorKind::AlreadyExists => {
                Status::failed_precondition("Something already exists with that name")
            }
            ErrorKind::NotFound => Status::failed_precondition(
                "Can't create directory, parent directory doesn't exist",
            ),
            _ => unexpected("cannot create", &directory, &e),
        })
    }

    /// Deletes a file, an empty directory, or a link (not what it points to).
    ///
    /// Behavior: `FilesystemStorageServiceGrpcImpl#deleteItem`, `java.nio.file.Files#delete`
    pub(super) fn delete(&self, path: &str) -> Result<(), Status> {
        let item = self
            .resolve(path)?
            .not_root("Can't delete the root directory")?
            .confined(Last::Itself)?;
        let failed = |e: io::Error| match e.kind() {
            ErrorKind::NotFound => {
                Status::failed_precondition("Cannot delete, file does not exists")
            }
            ErrorKind::DirectoryNotEmpty => {
                Status::failed_precondition("Cannot delete non-empty directory")
            }
            _ => unexpected("cannot delete", &item, &e),
        };
        if fs::symlink_metadata(&item).map_err(failed)?.is_dir() {
            fs::remove_dir(&item).map_err(failed)
        } else {
            fs::remove_file(&item).map_err(failed)
        }
    }
}

/// A client's path that passed Java's check.
struct Resolved<'a> {
    storage: &'a StorageRoot,
    /// The path as the client sent it, without its leading `/`.
    incoming: String,
    path: PathBuf,
}

impl Resolved<'_> {
    /// Behavior: `FilesystemStorageServiceGrpcImpl#requireNotRoot`
    fn not_root(self, message: &str) -> Result<Self, Status> {
        if self.path == self.storage.root {
            return Err(Status::failed_precondition(message));
        }
        Ok(self)
    }

    /// The path, if the file system stays in the storage when it follows it.
    fn confined(self, last: Last) -> Result<PathBuf, Status> {
        if !self.storage.holds(&self.path, last) {
            return Err(Status::invalid_argument(format!(
                "Invalid path: {}",
                self.incoming
            )));
        }
        Ok(self.path)
    }
}
