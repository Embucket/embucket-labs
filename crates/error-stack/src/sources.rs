use std::error::Error;

// Code taken from stable-x86_64-unknown-linux-gnu/lib/rustlib/src/rust/library/core/src/error.rs
// as originally it depends on unstable feature error_iter

/// An iterator over an [`Error`] and its sources.
///
/// If you want to omit the initial error and only process
/// its sources, use `skip(1)`.
#[derive(Clone, Debug)]
pub struct Source<'a> {
    pub current: Option<&'a (dyn Error + 'static)>,
}

impl<'a> Iterator for Source<'a> {
    type Item = &'a (dyn Error + 'static);

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current;
        self.current = self.current.and_then(Error::source);
        current
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.current.is_some() {
            (1, None)
        } else {
            (0, Some(0))
        }
    }
}
