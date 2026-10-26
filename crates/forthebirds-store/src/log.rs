use std::{fmt::Arguments, io::Write};

/// Write one whole line to stderr in one `write_all`.
pub(crate) fn line(args: Arguments<'_>) {
    let mut line = args.to_string();
    line.push('\n');
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}
