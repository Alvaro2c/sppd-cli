use crate::errors::AppResult;
use quick_xml::events::Event;

use super::scope::{ContractFolderStatusScope, ScopeResult};

/// Result produced when a `<ContractFolderStatus>` subtree finishes.
pub type ParsedContractFolderStatus = ScopeResult;

/// Handles events inside `<ContractFolderStatus>`.
pub struct ContractFolderStatusHandler {
    scope: Option<ContractFolderStatusScope>,
}

impl ContractFolderStatusHandler {
    pub fn new() -> Self {
        Self { scope: None }
    }

    pub fn reset(&mut self) {
        self.scope = None;
    }

    pub fn is_active(&self) -> bool {
        self.scope.is_some()
    }

    pub fn start(&mut self, event: Event) -> AppResult<()> {
        self.scope = Some(ContractFolderStatusScope::start(event)?);
        Ok(())
    }

    pub fn handle_event(&mut self, event: Event) -> AppResult<()> {
        if let Some(scope) = self.scope.as_mut() {
            scope.handle_event(event)?;
        }
        Ok(())
    }

    pub fn handle_end(&mut self, event: Event) -> AppResult<Option<ParsedContractFolderStatus>> {
        match self.scope.take() {
            Some(scope) => Ok(Some(scope.finish(event)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start_event() -> Event<'static> {
        Event::Start(quick_xml::events::BytesStart::new("ContractFolderStatus"))
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

    #[test]
    fn captures_project_name() {
        let mut handler = ContractFolderStatusHandler::new();
        handler.start(start_event()).unwrap();
        handler
            .handle_event(Event::Start(quick_xml::events::BytesStart::new(
                "cac:ProcurementProject",
            )))
            .unwrap();
        handler
            .handle_event(Event::Start(quick_xml::events::BytesStart::new("cbc:Name")))
            .unwrap();
        handler
            .handle_event(Event::Text(quick_xml::events::BytesText::new(
                "Project Alpha",
            )))
            .unwrap();
        handler
            .handle_event(Event::End(quick_xml::events::BytesEnd::new("cbc:Name")))
            .unwrap();
        handler
            .handle_event(Event::End(quick_xml::events::BytesEnd::new(
                "cac:ProcurementProject",
            )))
            .unwrap();

        let captured = handler
            .handle_end(Event::End(quick_xml::events::BytesEnd::new(
                "ContractFolderStatus",
            )))
            .unwrap()
            .expect("expected captured data");

        assert_eq!(captured.cfs_project_name, Some("Project Alpha".to_string()));
        assert!(captured
            .cfs_raw_xml
            .contains("<cbc:Name>Project Alpha</cbc:Name>"));
    }

    #[test]
    fn captures_status_code() {
        let mut handler = ContractFolderStatusHandler::new();
        handler.start(start_event()).unwrap();
        handler
            .handle_event(Event::Start(quick_xml::events::BytesStart::new(
                "cbc-place-ext:ContractFolderStatusCode",
            )))
            .unwrap();
        handler
            .handle_event(Event::Text(quick_xml::events::BytesText::new("200")))
            .unwrap();
        handler
            .handle_event(Event::End(quick_xml::events::BytesEnd::new(
                "cbc-place-ext:ContractFolderStatusCode",
            )))
            .unwrap();

        let captured = handler
            .handle_end(Event::End(quick_xml::events::BytesEnd::new(
                "ContractFolderStatus",
            )))
            .unwrap()
            .expect("expected captured data");

        assert_eq!(captured.cfs_status_code, Some("200".to_string()));
    }

    #[test]
    fn captures_id() {
        let mut handler = ContractFolderStatusHandler::new();
        handler.start(start_event()).unwrap();
        handler
            .handle_event(Event::Start(quick_xml::events::BytesStart::new(
                "cbc:ContractFolderID",
            )))
            .unwrap();
        handler
            .handle_event(Event::Text(quick_xml::events::BytesText::new("ID-42")))
            .unwrap();
        handler
            .handle_event(Event::End(quick_xml::events::BytesEnd::new(
                "cbc:ContractFolderID",
            )))
            .unwrap();

        let captured = handler
            .handle_end(Event::End(quick_xml::events::BytesEnd::new(
                "ContractFolderStatus",
            )))
            .unwrap()
            .expect("expected captured data");

        assert_eq!(captured.cfs_id, Some("ID-42".to_string()));
    }
}
