use crate::errors;
use arrow_schema::DataType;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::error::Result as DFResult;
use datafusion_common::cast::as_generic_string_array;
use regex::{CaptureMatches, Regex, RegexBuilder};

pub fn pattern_to_regex(pattern: &str, regexp_parameters: &str) -> Result<Regex, regex::Error> {
    //Snowflake registers only the last c or i in the sequence of regexp_parameters
    let case_insensitive = regexp_parameters
        .chars()
        .rev()
        .find(|&ch| ch == 'i' || ch == 'c')
        == Some('i');
    RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .multi_line(regexp_parameters.contains('m'))
        .dot_matches_new_line(regexp_parameters.contains('s'))
        .build()
}

pub fn regexp<'h, 'r: 'h>(
    array: &'h StringArray,
    regex: &'r Regex,
    position: usize,
) -> impl Iterator<Item = Option<CaptureMatches<'r, 'h>>> {
    array
        .iter()
        .map(move |opt| opt.map(move |s| regex.captures_iter(&s[position.min(s.len())..])))
}

pub fn to_string_array(array: &dyn Array) -> DFResult<StringArray> {
    match array.data_type() {
        DataType::Utf8 => Ok(as_generic_string_array(array)?.clone()),
        DataType::Utf8View | DataType::LargeUtf8 => {
            let casted = cast(array, &DataType::Utf8)?;
            Ok(as_generic_string_array(&casted)?.clone())
        }
        other => errors::UnsupportedInputTypeSnafu {
            data_type: other.clone(),
        }
        .fail()?,
    }
}
