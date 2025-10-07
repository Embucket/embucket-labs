#[derive(Clone, Debug)]
pub struct SlatedbVfsHandle {
    pub path: String,
    readonly: bool,
    pub handle_id: u64,
}

impl SlatedbVfsHandle {
    pub const fn new(path: String, readonly: bool, handle_id: u64) -> Self {
        Self { path, readonly, handle_id }
    }
}

impl sqlite_plugin::vfs::VfsHandle for SlatedbVfsHandle {
    fn readonly(&self) -> bool {
        self.readonly
    }

    fn in_memory(&self) -> bool {
        // TODO does this matter?
        false
    }
}
