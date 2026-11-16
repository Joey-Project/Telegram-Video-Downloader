use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    SignedOut,
    WaitingForScan,
    WaitingForConfirmation,
    Connected,
    Expired,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Mode {
    Web,
    Tv,
    AccessKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub(crate) struct CredentialSummary {
    pub(crate) has_cookie: bool,
    pub(crate) has_access_key: bool,
    pub(crate) has_tv_access_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Snapshot {
    pub(crate) phase: Phase,
    pub(crate) mode: Option<Mode>,
    pub(crate) generation: u64,
    pub(crate) credential_revision: u64,
    pub(crate) credential_summary: CredentialSummary,
    pub(crate) updated_at_ms: u64,
    pub(crate) safe_reason: &'static str,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            phase: Phase::SignedOut,
            mode: None,
            generation: 0,
            credential_revision: 0,
            credential_summary: CredentialSummary::default(),
            updated_at_ms: 0,
            safe_reason: "none",
        }
    }
}

impl Snapshot {
    pub(crate) fn transition(
        &mut self,
        generation: u64,
        phase: Phase,
        reason: &'static str,
        updated_at_ms: u64,
    ) -> bool {
        if self.generation > generation {
            return false;
        }
        self.generation = generation;
        self.phase = phase;
        self.safe_reason = reason;
        self.updated_at_ms = updated_at_ms;
        true
    }

    pub(crate) fn set_mode(&mut self, mode: Mode) {
        self.mode = Some(mode);
    }

    pub(crate) fn set_credentials(
        &mut self,
        generation: u64,
        credential_revision: u64,
        summary: CredentialSummary,
        updated_at_ms: u64,
    ) -> bool {
        if self.generation != generation || self.credential_revision > credential_revision {
            return false;
        }
        self.credential_revision = credential_revision;
        self.credential_summary = summary;
        self.updated_at_ms = updated_at_ms;
        true
    }
}

pub(crate) fn reason_is_safe(reason: &str) -> bool {
    matches!(
        reason,
        "none"
            | "duplicate_start"
            | "login_expired"
            | "login_cancelled"
            | "login_failed"
            | "credentials_removed"
            | "stale_generation"
            | "stale_credentials"
            | "logout_failed"
            | "invalid_request"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_completion_cannot_replace_a_newer_phase_or_credentials() {
        let mut state = Snapshot::default();
        assert!(state.transition(4, Phase::WaitingForScan, "none", 10));
        assert!(state.transition(6, Phase::Cancelled, "login_cancelled", 20));
        assert!(!state.transition(4, Phase::Connected, "none", 30));
        assert!(!state.set_credentials(
            4,
            1,
            CredentialSummary {
                has_cookie: true,
                ..Default::default()
            },
            30
        ));
        assert_eq!(state.phase, Phase::Cancelled);
        assert_eq!(state.credential_summary, CredentialSummary::default());
    }

    #[test]
    fn only_bounded_reason_codes_are_accepted() {
        assert!(reason_is_safe("login_failed"));
        assert!(!reason_is_safe("remote response with token"));
    }

    #[test]
    fn snapshot_serialization_is_a_safe_allowlist() {
        let state = Snapshot {
            phase: Phase::Connected,
            mode: Some(Mode::Web),
            generation: 8,
            credential_revision: 2,
            credential_summary: CredentialSummary {
                has_cookie: true,
                has_access_key: false,
                has_tv_access_key: false,
            },
            updated_at_ms: 123,
            safe_reason: "none",
        };
        let value = serde_json::to_value(state).expect("snapshot should serialize");
        let object = value.as_object().expect("snapshot should be an object");
        assert_eq!(
            object
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            [
                "credential_revision",
                "credential_summary",
                "generation",
                "mode",
                "phase",
                "safe_reason",
                "updated_at_ms",
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(value["phase"], "connected");
        assert_eq!(value["credential_summary"]["has_cookie"], true);
    }
}
