use crate::frame::Frame;

#[derive(Debug, Clone)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub fn new(command_name: impl Into<String>) -> Self {
        Self {
            command_name: command_name.into(),
        }
    }

    pub fn apply(&self) -> Frame {
        Frame::Error(format!("ERR unknown command '{}'", self.command_name))
    }
}
