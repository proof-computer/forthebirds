//! Format each stderr log line before writing it to the shared sink.

use std::{fmt::Arguments, io::Write};

/// Emit a complete line while holding the stderr lock.
pub fn emit(args: Arguments<'_>) {
    emit_to(&mut std::io::stderr().lock(), args);
}

/// Submit the formatted line and its terminator in one `write_all` call.
///
/// Logging is best effort: a failed sink must not take the process down.
pub fn emit_to<W: Write>(sink: &mut W, args: Arguments<'_>) {
    let mut line = args.to_string();
    line.push('\n');
    let _ = sink.write_all(line.as_bytes());
}

/// Write one formatted log line to stderr.
#[macro_export]
macro_rules! log_line {
    ($($arg:tt)*) => {
        $crate::log::emit(format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[derive(Default)]
    struct CountingWriter {
        calls: Vec<Vec<u8>>,
    }

    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.calls.push(bytes.to_vec());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn assert_single_line(args: Arguments<'_>, expected: &[u8]) {
        let mut sink = CountingWriter::default();
        emit_to(&mut sink, args);
        assert_eq!(sink.calls.len(), 1);
        assert_eq!(sink.calls[0], expected);
        assert_eq!(expected.last(), Some(&b'\n'));
        assert!(!sink.calls[0][..expected.len() - 1].contains(&b'\n'));
    }

    #[test]
    fn formatted_arguments_reach_the_sink_in_one_write() {
        assert_single_line(
            format_args!(
                "planetarium launch stage=payload-part index={} of={}",
                7, 11
            ),
            b"planetarium launch stage=payload-part index=7 of=11\n",
        );
    }

    #[test]
    fn literal_line_reaches_the_sink_in_one_write() {
        assert_single_line(
            format_args!("planetarium deployment stage=stopped"),
            b"planetarium deployment stage=stopped\n",
        );
    }

    #[test]
    fn sink_failure_does_not_panic() {
        struct BrokenWriter;
        impl Write for BrokenWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        emit_to(
            &mut BrokenWriter,
            format_args!("planetarium log sink failed"),
        );
    }
}
