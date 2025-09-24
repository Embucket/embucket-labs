use crate::error::StackError;

// break up error into pieces, it does it more fancy than error_stack_trace
macro_rules! get_error_chain {
    ($err:expr) => {{
        let mut err: &dyn std::error::Error = $err;
        let mut i = 0;
        let mut lines = Vec::new();
        loop {
            lines.push(format!("{i}: {err}"));
            if let Some(source) = err.source() {
                err = source;
                i += 1;
            } else {
                break;
            }
        }
        lines.join("\n")
    }};
}

pub trait ErrorChainExt: StackError {
    fn error_chain(&self) -> String {
        get_error_chain!(&self)
    }
}

impl<T> ErrorChainExt for T where T: StackError {}
