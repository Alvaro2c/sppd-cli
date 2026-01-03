use crate::errors::{AppError, AppResult};
use quick_xml::events::Event;
use quick_xml::writer::Writer;
use quick_xml_to_json::xml_to_json;
use std::io::Cursor;

/// State for capturing ContractFolderStatus XML subtree
struct ContractFolderStatusState {
    depth: u32,
    buffer: Vec<u8>,
    found: bool,
}

/// Handler for parsing ContractFolderStatus XML subtrees.
///
/// This handler captures the entire ContractFolderStatus XML element and converts
/// it to JSON format. It tracks nesting depth to properly handle the complete
/// XML subtree.
pub struct ContractFolderStatusHandler {
    state: Option<ContractFolderStatusState>,
}

impl ContractFolderStatusHandler {
    /// Creates a new ContractFolderStatusHandler.
    pub fn new() -> Self {
        Self { state: None }
    }

    /// Resets the handler to initial state.
    pub fn reset(&mut self) {
        self.state = None;
    }

    /// Returns true if currently capturing a ContractFolderStatus element.
    pub fn is_active(&self) -> bool {
        self.state.is_some()
    }

    /// Starts capturing a ContractFolderStatus element.
    ///
    /// Returns an error if a ContractFolderStatus element is already being captured.
    pub fn start(&mut self, event: Event) -> AppResult<()> {
        if let Some(ref state) = self.state {
            if state.found {
                return Err(AppError::ParseError(
                    "Multiple ContractFolderStatus elements found in entry".to_string(),
                ));
            }
        }

        // Pre-allocate buffer with estimated capacity (typically ContractFolderStatus is 1-10KB)
        let mut buffer = Vec::with_capacity(4096);
        {
            let mut writer = Writer::new(&mut buffer);
            writer.write_event(event).map_err(|e| {
                AppError::ParseError(format!("Failed to write event to buffer: {e}"))
            })?;
        }

        let state = ContractFolderStatusState {
            depth: 1,
            buffer,
            found: true,
        };

        self.state = Some(state);
        Ok(())
    }

    /// Handles an event while inside ContractFolderStatus (generic event).
    pub fn handle_event(&mut self, event: Event) -> AppResult<()> {
        if let Some(ref mut state) = self.state {
            let mut writer = Writer::new(&mut state.buffer);
            writer.write_event(event).map_err(|e| {
                AppError::ParseError(format!("Failed to write event to buffer: {e}"))
            })?;
        }
        Ok(())
    }

    /// Handles a start tag event while inside ContractFolderStatus.
    pub fn handle_start(&mut self, event: Event) -> AppResult<()> {
        if let Some(ref mut state) = self.state {
            state.depth += 1;
            let mut writer = Writer::new(&mut state.buffer);
            writer.write_event(event).map_err(|e| {
                AppError::ParseError(format!("Failed to write event to buffer: {e}"))
            })?;
        }
        Ok(())
    }

    /// Handles an end tag event while inside ContractFolderStatus.
    ///
    /// Returns `Some(String)` with the JSON representation when the element is complete,
    /// or `None` if still capturing nested elements.
    pub fn handle_end(&mut self, event: Event) -> AppResult<Option<String>> {
        if let Some(ref mut state) = self.state {
            let mut writer = Writer::new(&mut state.buffer);
            writer.write_event(event).map_err(|e| {
                AppError::ParseError(format!("Failed to write event to buffer: {e}"))
            })?;

            state.depth -= 1;

            if state.depth == 0 {
                // Convert XML buffer to JSON
                let mut json_output = Vec::with_capacity(state.buffer.len());
                let mut cursor = Cursor::new(&state.buffer);
                xml_to_json(&mut cursor, &mut json_output).map_err(|e| {
                    AppError::ParseError(format!(
                        "Failed to convert ContractFolderStatus to JSON: {e}"
                    ))
                })?;

                let json_string = String::from_utf8(json_output).map_err(|e| {
                    AppError::ParseError(format!("Failed to convert JSON to UTF-8: {e}"))
                })?;

                self.state = None;
                Ok(Some(json_string))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::events::{BytesStart, Event};

    fn start_event() -> Event<'static> {
        Event::Start(BytesStart::new("ContractFolderStatus"))
    }

    #[test]
    fn start_marks_handler_active() {
        let mut handler = ContractFolderStatusHandler::new();
        handler.start(start_event()).unwrap();
        assert!(handler.is_active());
    }

    #[test]
    fn reset_marks_handler_inactive() {
        let mut handler = ContractFolderStatusHandler::new();
        handler.start(start_event()).unwrap();
        handler.reset();
        assert!(!handler.is_active());
    }
}
