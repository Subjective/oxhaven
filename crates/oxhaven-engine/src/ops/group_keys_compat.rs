//! The interface of the aggregations' former key table (`ops/group_keys.rs`, task 9.2) over
//! [`KeyTable`], so that the tests written for that table run unchanged against the table
//! that replaced it.

pub(super) use super::*;

pub(super) struct GroupKeys {
    table: KeyTable,
    pub(super) collide: bool,
}

impl GroupKeys {
    pub(super) fn new(data_types: &[DataType]) -> Result<GroupKeys, TableFailure> {
        Ok(GroupKeys {
            table: KeyTable::for_types(data_types)?,
            collide: false,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    pub(super) fn find_or_create(
        &mut self,
        key_columns: &[ArrayRef],
        slots: &mut Vec<usize>,
        created: &mut Vec<usize>,
    ) -> Result<(), TableFailure> {
        self.table.collide = self.collide;
        self.table.find_or_create(key_columns, slots, created)
    }

    pub(super) fn find(
        &mut self,
        key_columns: &[ArrayRef],
        slots: &mut Vec<usize>,
    ) -> Result<bool, TableFailure> {
        self.table.collide = self.collide;
        self.table.find_all(key_columns, slots)
    }
}

#[path = "group_keys_tests.rs"]
mod tests;
