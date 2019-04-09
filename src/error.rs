quick_error! {
     #[derive(Debug)]
     pub enum ReuquestError {
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

quick_error! {
     #[derive(Debug)]
     pub enum ResponseError {
        Hyper(err: hyper::error::Error) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum BuildError {
        InvalidHeader(err: http::Error) {
             from()
        }
        RequiredField(err: std::string::String) {
             from()
        }
     }
}