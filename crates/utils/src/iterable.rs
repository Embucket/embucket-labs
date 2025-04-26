use bytes::Bytes;

pub trait IterableCursor {
    const CURSOR_MIN: Self;
    const CURSOR_MAX: Self;

    #[must_use]
    fn next_cursor(&self) -> Self;
    fn as_bytes(&self) -> Bytes;
}

#[allow(clippy::trait_duplication_in_bounds)]
impl IterableCursor for i64 {
    const CURSOR_MIN: Self = 0;
    const CURSOR_MAX: Self = Self::MAX;

    fn next_cursor(&self) -> Self {
        if self < &Self::CURSOR_MAX {
            self + 1
        } else {
            Self::CURSOR_MIN
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
        Self::Cursor::CURSOR_MIN
    }

    #[must_use]
    fn max_cursor() -> Self::Cursor {
        Self::Cursor::CURSOR_MAX
    }

    fn next_cursor(&self) -> Self::Cursor {
        self.cursor().next_cursor()
    }

    fn cursor_bytes(&self) -> Bytes {
        Bytes::from(self.cursor().to_string())
    }
}
