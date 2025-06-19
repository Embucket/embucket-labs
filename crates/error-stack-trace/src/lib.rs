mod stack;

use proc_macro::TokenStream;
use stack::stack_trace_style_impl;

#[proc_macro_attribute]
pub fn debug(args: TokenStream, input: TokenStream) -> TokenStream {
    stack_trace_style_impl(args.into(), input.into()).into()
}
