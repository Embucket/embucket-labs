use bytes::Bytes;

pub trait IterableCursor {
    #[must_use]
    fn next_cursor(&self) -> Self;
    #[must_use]
    fn min_cursor() -> Self;
    #[must_use]
    fn max_cursor() -> Self;

    fn as_bytes(&self) -> Bytes;
}

#[allow(clippy::trait_duplication_in_bounds)]
impl IterableCursor for i64 {
    fn min_cursor() -> Self {
        0
    }

    fn max_cursor() -> Self {
        Self::MAX
    }

    fn next_cursor(&self) -> Self {
        if *self < Self::max_cursor() {
            self + 1
        } else {
            Self::min_cursor()
        }
    }

    fn as_bytes(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

pub trait IterableEntity {
    type Cursor: IterableCursor + ToString;

    fn cursor(&self) -> Self::Cursor;

    fn key(&self) -> Bytes;

    #[must_use]
    fn min_cursor() -> Self::Cursor {
        Self::Cursor::min_cursor()
    }

    #[must_use]
    fn max_cursor() -> Self::Cursor {
        Self::Cursor::max_cursor()
    }

    fn next_cursor(&self) -> Self::Cursor {
        self.cursor().next_cursor()
    }

    fn cursor_bytes(&self) -> Bytes {
        Bytes::from(self.cursor().to_string())
    }
}
