use std::fmt;

#[derive(Debug)]
pub struct SocketError {
    message: String,
}

impl SocketError {
    pub fn new(descr: String) -> SocketError {
        return SocketError { message: descr };
    }
}

impl fmt::Display for SocketError {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        write!(f, "{}", self.message)
    }
}
