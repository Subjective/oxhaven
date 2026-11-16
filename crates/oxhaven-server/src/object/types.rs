//! Object types: what the server knows how to hand to a client besides a table.
//!
//! An [`ObjectType`] says whether a value is of the type and opens a connection to one: it
//! is given a [`MessageStream`] to the client and answers with the stream that takes the
//! client's messages. A type whose objects are only fetched sends one message and closes
//! ([`send_once`]).
//!
//! [`registry`] holds the types of the process: the built-in ones, and those that Python
//! plugins register when the server starts (`console::python::plugin`). There is one per
//! process, as there is one Python interpreter per process; Java's is a singleton too.
//! `docs/OBJECTS.md` is the design.
//!
//! Behavior: `io.deephaven.plugin.type.ObjectType`, `io.deephaven.plugin.type.ObjectTypeBase`,
//! `io.deephaven.server.plugin.type.ObjectTypes`, `io.deephaven.server.object.TypeLookup`

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use tonic::Status;

use crate::hierarchical::hierarchical_of;
use crate::partitioned::{describe, partitioned_of};
use crate::session::{ExportError, ExportValue};

/// The type name of a table, which is no object type: a client opens a table with the
/// table service.
///
/// Behavior: `TypeLookup#type`
pub const TABLE_TYPE_NAME: &str = "Table";

/// One direction of the messages between a client and an object: the server is handed
/// one to send to the client with, and makes one that takes what the client sends.
///
/// Behavior: `io.deephaven.plugin.type.ObjectType.MessageStream`
pub trait MessageStream: Send + Sync {
    /// A message: bytes that only the type and its client understand, and the server's
    /// objects they refer to.
    fn on_data(&self, payload: Vec<u8>, references: Vec<ExportValue>) -> Result<(), ExportError>;

    /// The end of the stream, in both directions.
    fn on_close(&self);
}

/// The stream of an object that takes no messages.
///
/// Behavior: `ObjectType.MessageStream#NOOP`
pub struct Noop;

impl MessageStream for Noop {
    fn on_data(&self, _payload: Vec<u8>, _references: Vec<ExportValue>) -> Result<(), ExportError> {
        Ok(())
    }

    fn on_close(&self) {}
}

/// A kind of object that clients can open.
///
/// Behavior: `io.deephaven.plugin.type.ObjectType`
pub trait ObjectType: Send + Sync {
    /// What clients are told the type of such an object is, and name it by.
    fn name(&self) -> &str;

    /// Whether `value` is of this type. An error fails whatever asked: a fetch, and the
    /// look at the variables of the script session that found the value.
    fn is_type(&self, value: &ExportValue) -> Result<bool, ExportError>;

    /// Opens `value`, which is of this type, for a client that is reached through
    /// `connection`, and returns what takes the messages of that client. Before it
    /// returns it sends the client what it needs to show the object.
    ///
    /// Behavior: `ObjectTypeBase#compatibleClientConnection`
    fn connect(
        &self,
        value: &ExportValue,
        connection: Arc<dyn MessageStream>,
    ) -> Result<Arc<dyn MessageStream>, ExportError>;
}

/// The whole connection of an object that is only fetched: one message, and the end.
///
/// Behavior: `ObjectTypeBase.FetchOnly#compatibleClientConnection`
pub fn send_once(
    connection: &dyn MessageStream,
    payload: Vec<u8>,
    references: Vec<ExportValue>,
) -> Result<Arc<dyn MessageStream>, ExportError> {
    connection.on_data(payload, references)?;
    connection.on_close();
    Ok(Arc::new(Noop))
}

/// A partitioned table, as clients open one: its descriptor, and a reference to the table
/// of its constituents.
///
/// Behavior: `io.deephaven.partitionedtable.PartitionedTableTypePlugin`
struct PartitionedTableType;

impl ObjectType for PartitionedTableType {
    fn name(&self) -> &str {
        crate::partitioned::TYPE_NAME
    }

    fn is_type(&self, value: &ExportValue) -> Result<bool, ExportError> {
        Ok(partitioned_of(value).is_some())
    }

    fn connect(
        &self,
        value: &ExportValue,
        connection: Arc<dyn MessageStream>,
    ) -> Result<Arc<dyn MessageStream>, ExportError> {
        let partitioned = partitioned_of(value)
            .ok_or_else(|| ExportError::Internal("not a partitioned table".to_string()))?;
        let (payload, table) = describe(partitioned)?;
        send_once(
            connection.as_ref(),
            payload,
            vec![ExportValue::Table(table)],
        )
    }
}

/// A hierarchical table, as clients open one: its descriptor. The rows are read through a
/// view of it (`crate::hierarchical`), so the message refers to nothing.
///
/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableTypePlugin`
struct HierarchicalTableType;

impl ObjectType for HierarchicalTableType {
    fn name(&self) -> &str {
        crate::hierarchical::TYPE_NAME
    }

    fn is_type(&self, value: &ExportValue) -> Result<bool, ExportError> {
        Ok(hierarchical_of(value).is_some())
    }

    fn connect(
        &self,
        value: &ExportValue,
        connection: Arc<dyn MessageStream>,
    ) -> Result<Arc<dyn MessageStream>, ExportError> {
        let hierarchical = hierarchical_of(value)
            .ok_or_else(|| ExportError::Internal("not a hierarchical table".to_string()))?;
        send_once(
            connection.as_ref(),
            crate::hierarchical::describe(hierarchical),
            Vec::new(),
        )
    }
}

/// The names no type may have: a client opens these kinds through services of their own.
///
/// Behavior: `ObjectTypes#RESERVED_TYPE_NAMES_LOWERCASE`
const RESERVED_NAMES_LOWERCASE: [&str; 4] = ["table", "rolluptable", "treetable", ""];

/// The types Java registers before any plugin and this server does not have yet. A plugin
/// cannot take their names in Java, so it cannot here; the task that adds one of them
/// registers it in [`registry`] and takes its name out of this list.
const JAVA_TYPES_NOT_PORTED_LOWERCASE: [&str; 1] = ["figure"];

/// The words `javax.lang.model.SourceVersion#isKeyword` answers true for (Java 21).
const JAVA_KEYWORDS: [&str; 54] = [
    "abstract",
    "assert",
    "boolean",
    "break",
    "byte",
    "case",
    "catch",
    "char",
    "class",
    "const",
    "continue",
    "default",
    "do",
    "double",
    "else",
    "enum",
    "extends",
    "final",
    "finally",
    "float",
    "for",
    "goto",
    "if",
    "implements",
    "import",
    "instanceof",
    "int",
    "interface",
    "long",
    "native",
    "new",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "short",
    "static",
    "strictfp",
    "super",
    "switch",
    "synchronized",
    "this",
    "throw",
    "throws",
    "transient",
    "try",
    "void",
    "volatile",
    "while",
    "true",
    "false",
    "null",
    "_",
];

#[derive(Default)]
struct State {
    names_lowercase: HashSet<String>,
    /// In the order they were registered, which is the order they are asked in.
    types: Vec<Arc<dyn ObjectType>>,
}

/// The object types of a server, and the lookup of the type of a value.
///
/// Behavior: `io.deephaven.server.plugin.type.ObjectTypes`
#[derive(Default)]
pub struct ObjectTypes {
    state: Mutex<State>,
}

impl ObjectTypes {
    /// No types at all; [`registry`] is the one with the server's.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a type. Its name must not be a Java keyword, a reserved name or the name of a
    /// type there is already, whatever the case of its letters; the error is the text of
    /// Java's `IllegalArgumentException`.
    ///
    /// Behavior: `ObjectTypes#register`
    pub fn register(&self, object_type: Arc<dyn ObjectType>) -> Result<(), String> {
        let name = object_type.name().to_string();
        // Java: toLowerCase(Locale.ENGLISH)
        let lowercase = name.to_lowercase();
        if JAVA_KEYWORDS.contains(&lowercase.as_str()) {
            return Err(format!("Unable to register type, name is keyword: {name}"));
        }
        if RESERVED_NAMES_LOWERCASE.contains(&lowercase.as_str()) {
            return Err(format!("Unable to register type, name is reserved: {name}"));
        }
        let mut state = self.lock();
        if state.names_lowercase.contains(&lowercase)
            || JAVA_TYPES_NOT_PORTED_LOWERCASE.contains(&lowercase.as_str())
        {
            return Err(format!(
                "Unable to register type, type name already registered: {name}"
            ));
        }
        state.names_lowercase.insert(lowercase);
        state.types.push(object_type);
        Ok(())
    }

    /// The names of the types, in the order they were registered.
    pub fn names(&self) -> Vec<String> {
        let types = self.lock().types.clone();
        types.iter().map(|t| t.name().to_string()).collect()
    }

    /// The first type, in the order of registration, that `value` is of.
    ///
    /// Behavior: `ObjectTypes#findObjectType`
    pub fn find(&self, value: &ExportValue) -> Result<Option<Arc<dyn ObjectType>>, ExportError> {
        // not under the lock: a type of a plugin runs Python to answer
        let types = self.lock().types.clone();
        for object_type in types {
            if object_type.is_type(value)? {
                return Ok(Some(object_type));
            }
        }
        Ok(None)
    }

    /// The type name clients are told for `value`: `Table` for a table, the name of its
    /// object type for an object that has one, and none for anything else.
    ///
    /// Behavior: `TypeLookup#type`
    pub fn type_name(&self, value: &ExportValue) -> Result<Option<String>, ExportError> {
        if value.as_table().is_some() {
            return Ok(Some(TABLE_TYPE_NAME.to_string()));
        }
        Ok(self.find(value)?.map(|t| t.name().to_string()))
    }

    /// The type of `value`, which a client asked for by the name `expected`.
    ///
    /// Behavior: `ObjectServiceGrpcImpl#getObjectTypeInstance`
    pub fn instance(
        &self,
        expected: &str,
        value: &ExportValue,
    ) -> Result<Arc<dyn ObjectType>, ExportError> {
        let Some(object_type) = self.find(value)? else {
            return Err(Status::not_found(format!(
                "No ObjectType found, expected type '{expected}'"
            ))
            .into());
        };
        if object_type.name() != expected {
            return Err(Status::failed_precondition(format!(
                "Unexpected ObjectType, expected type '{expected}', actual type '{}'",
                object_type.name()
            ))
            .into());
        }
        Ok(object_type)
    }
}

/// The object types of this process: `PartitionedTable`, `HierarchicalTable`, and what
/// plugins registered.
///
/// Behavior: the singleton `ObjectTypes` of a Java server, after
/// `io.deephaven.server.plugin.PluginRegistration#registerAll`
pub fn registry() -> &'static ObjectTypes {
    static REGISTRY: OnceLock<ObjectTypes> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let types = ObjectTypes::new();
        types
            .register(Arc::new(PartitionedTableType))
            .expect("the first type of an empty registry");
        types
            .register(Arc::new(HierarchicalTableType))
            .expect("a type that no other has the name of");
        types
    })
}
