quick_error! {
     #[derive(Debug)]
     pub enum RequestError {
        Build(err: http::Error) {
             from()
        }
        Body(err: BodyError) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum ResponseError {
        Build(err: RequestError) {
             from()
        }
        Send(err: hyper::error::Error) {
             from()
        }
        Utf8(err: std::string::FromUtf8Error) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum BodyError {
        Json(err: serde_json::Error) {
             from()
        }
        Gzip(err: std::io::Error) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum TemplateError {
        InvalidHeader(err: http::Error) {
             from()
        }
        RequiredField(err: std::string::String) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum ParamsError {
        RequiredField(err: std::string::String) {
             from()
        }
     }
}

quick_error! {
     #[derive(Debug)]
     pub enum LineError {
        RequiredField(err: std::string::String) {
             from()
        }
     }
}