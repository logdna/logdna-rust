quick_error! {
     #[derive(Debug)]
     pub enum Error {
        Json(err: serde_json::Error) {
             from()
        }
        Http(err: http::Error) {
             from()
        }
        Hyper(err: hyper::error::Error) {
             from()
        }
        Utf8(err: std::string::FromUtf8Error) {
             from()
        }
        Io(err: std::io::Error) {
             from()
        }
     }
}