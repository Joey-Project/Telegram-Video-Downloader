use std::time::Duration;

use anyhow::Error;

/// The action to take after one selected Bilibili episode fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EpisodeFailureDisposition {
    /// Retry this episode after `delay`, optionally requesting a fresh play plan.
    Retry { delay: Duration, refresh_plan: bool },
    /// Keep processing the group, but leave this episode failed.
    FailEntry,
    /// Stop processing the group because a shared prerequisite or safety check failed.
    PauseGroup,
}

/// Maximum number of episode-level retries after the existing SDK file retries.
pub const MAX_ADDITIONAL_EPISODE_RETRIES: usize = 2;

const NETWORK_RETRY_DELAY: Duration = Duration::from_secs(5);
const UPSTREAM_RETRY_DELAY: Duration = Duration::from_secs(20);

/// Classifies a Bilibili episode failure using its full `anyhow` error chain.
///
/// `retries_used` counts episode-level retries already started for this entry;
/// it does not include retries performed inside the BBDown SDK.
pub fn classify_episode_failure(error: &Error, retries_used: usize) -> EpisodeFailureDisposition {
    classify_episode_failure_with_retry_after(error, retries_used, None)
}

/// Classifies a Bilibili episode failure, using a parsed HTTP `Retry-After` when supplied.
///
/// The caller may pass a delay extracted from a typed response/header. This module does not
/// attempt to parse headers from error text. A supplied delay is used for HTTP 429 retries.
pub fn classify_episode_failure_with_retry_after(
    error: &Error,
    retries_used: usize,
    retry_after: Option<Duration>,
) -> EpisodeFailureDisposition {
    let chain = format!("{error:#}").to_ascii_lowercase();

    if is_shared_output_recovery_failure(&chain)
        || is_unsafe_publication_or_identity_failure(&chain)
        || is_shared_storage_failure(error, &chain)
        || is_group_authentication_failure(&chain)
    {
        return EpisodeFailureDisposition::PauseGroup;
    }

    if has_http_status(&chain, 404) || is_episode_unavailable(&chain) {
        return EpisodeFailureDisposition::FailEntry;
    }

    if has_http_status(&chain, 403) {
        return retry_or_fail(retries_used, NETWORK_RETRY_DELAY, true);
    }

    if has_http_status(&chain, 429) {
        return retry_or_fail(
            retries_used,
            retry_after.unwrap_or(UPSTREAM_RETRY_DELAY),
            false,
        );
    }

    if has_http_5xx_status(&chain) {
        return retry_or_fail(retries_used, UPSTREAM_RETRY_DELAY, false);
    }

    if is_transient_network_failure(&chain) {
        return retry_or_fail(retries_used, NETWORK_RETRY_DELAY, false);
    }

    EpisodeFailureDisposition::FailEntry
}

fn retry_or_fail(
    retries_used: usize,
    delay: Duration,
    refresh_plan: bool,
) -> EpisodeFailureDisposition {
    if retries_used >= MAX_ADDITIONAL_EPISODE_RETRIES {
        EpisodeFailureDisposition::FailEntry
    } else {
        EpisodeFailureDisposition::Retry {
            delay,
            refresh_plan,
        }
    }
}

fn is_shared_output_recovery_failure(chain: &str) -> bool {
    chain.contains("video output recovery scan is incomplete")
}

fn is_group_authentication_failure(chain: &str) -> bool {
    has_http_status(chain, 401)
        || contains_any(
            chain,
            &[
                "login expired",
                "login has expired",
                "login is required",
                "not logged in",
                "authentication required",
                "unauthorized",
                "credential",
                "cookie expired",
                "expired cookie",
                "token expired",
                "expired token",
                "账号未登录",
                "未登录",
                "登录过期",
                "登录失效",
                "需要登录",
            ],
        )
        || contains_any(
            chain,
            &[
                "area restriction",
                "region restriction",
                "region-restricted",
                "unavailable in your region",
                "unavailable in your country",
                "geo-restricted",
                "地區限制",
                "地区限制",
                "区域限制",
                "地域限制",
                "仅限中国大陆",
            ],
        )
}

fn is_shared_storage_failure(error: &Error, chain: &str) -> bool {
    if contains_any(
        chain,
        &[
            "no space left on device",
            "disk is full",
            "disk full",
            "insufficient disk space",
            "read-only file system",
            "filesystem is read-only",
            "root permission",
            "enospc",
            "erofs",
        ],
    ) {
        return true;
    }

    let permission_failure = contains_any(
        chain,
        &[
            "permission denied",
            "operation not permitted",
            "eacces",
            "eperm",
            "os error 1",
            "os error 13",
            "os error 30",
        ],
    );
    permission_failure
        && (has_typed_local_permission_error(error)
            || contains_any(
                chain,
                &[
                    "output root",
                    "staging directory",
                    "local filesystem",
                    "local file system",
                    "write output",
                    "create output",
                    "open output",
                ],
            ))
}

fn has_typed_local_permission_error(error: &Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| {
                io_error.kind() == std::io::ErrorKind::PermissionDenied
                    || matches!(io_error.raw_os_error(), Some(1 | 13 | 30))
            })
    })
}

fn is_unsafe_publication_or_identity_failure(chain: &str) -> bool {
    contains_any(
        chain,
        &[
            "unsafe publication",
            "unsafe destination",
            "identity mismatch",
            "identity changed",
            "changed identity since it was saved",
            "bilibili group inventory changed since the reviewed plan",
            "bilibili group episode numbering changed since the reviewed plan",
            "bilibili media requirements changed since the reviewed plan",
            "saved bilibili group inventory no longer matches the current selection",
            "object identity changed",
            "path identity changed",
            "output root or staging directory changed",
            "output root changed after overwrite confirmation",
            "configured output root identity changed",
            "output root identity changed",
            "video staging root identity changed",
            "staging directory identity changed",
            "staging directory access path changed",
            "video staging ownership record changed during validation",
            "video staging ownership record does not match the bound output root",
            "video staging ownership record identity changed after validation",
            "video control ownership record does not match the bound output root",
            "video control ownership record identity changed after validation",
            "bound file descriptor identity changed",
            "bound directory descriptor identity changed",
            "bound directory identity changed",
            "bound file identity or permissions changed",
            "bound directory identity or private permissions changed",
            "bound output parent directory identity changed",
            "reserved bilibili mux output identity changed",
            "bilibili mux prepublication anchor identity changed",
            "published primary media object changed while rebuilding the final report",
            "published primary media is missing while rebuilding the final report",
            "published primary media is not a regular file while rebuilding the final report",
            "failed to validate moved destination through the configured output root",
            "published media source is outside its configured download root",
            "published media path is outside its configured download root",
            "publication path is outside the output root",
            "overwrite target changed after confirmation",
            "file was replaced",
            "file replacement detected",
            "source changed during publication",
        ],
    )
}

fn is_episode_unavailable(chain: &str) -> bool {
    contains_any(
        chain,
        &[
            "episode not found",
            "episode unavailable",
            "invalid episode",
            "video not found",
            "video unavailable",
            "content is unavailable",
            "该视频不存在",
            "视频不存在",
            "剧集不存在",
            "视频不可用",
        ],
    )
}

fn is_transient_network_failure(chain: &str) -> bool {
    contains_any(
        chain,
        &[
            "timed out",
            "timeout",
            "connection reset",
            "connection refused",
            "connection closed",
            "broken pipe",
            "failed to connect",
            "error sending request",
            "temporary failure in name resolution",
            "name or service not known",
            "network is unreachable",
            "network unreachable",
            "connection aborted",
        ],
    )
}

fn has_http_5xx_status(chain: &str) -> bool {
    (500..=599).any(|status| has_http_status(chain, status))
}

fn has_http_status(chain: &str, status: u16) -> bool {
    let status = status.to_string();
    chain.match_indices(&status).any(|(start, _)| {
        let before = &chain[..start];
        let requested_context_start = before.len().saturating_sub(48);
        let context_start = before
            .char_indices()
            .find_map(|(index, _)| (index >= requested_context_start).then_some(index))
            .unwrap_or(0);
        let context = &before[context_start..];
        let after = &chain[start + status.len()..];
        let has_status_context = contains_any(context, &["http", "status", "response", "code"]);
        let has_reason_phrase = match status.as_str() {
            "401" => after.trim_start().starts_with("unauthorized"),
            "403" => after.trim_start().starts_with("forbidden"),
            "404" => after.trim_start().starts_with("not found"),
            "429" => after.trim_start().starts_with("too many requests"),
            _ => {
                after.trim_start().starts_with("server error")
                    || after.trim_start().starts_with("service unavailable")
                    || after.trim_start().starts_with("bad gateway")
                    || after.trim_start().starts_with("gateway timeout")
                    || after.trim_start().starts_with("internal server error")
            }
        };
        let code_boundary = before
            .as_bytes()
            .last()
            .is_none_or(|byte| !byte.is_ascii_digit())
            && after
                .as_bytes()
                .first()
                .is_none_or(|byte| !byte.is_ascii_digit());

        code_boundary && (has_status_context || has_reason_phrase)
    })
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    fn classify(message: &str, retries_used: usize) -> EpisodeFailureDisposition {
        classify_episode_failure(&anyhow!("{message}"), retries_used)
    }

    #[test]
    fn retries_network_failures_then_exhausts_episode_budget() {
        let expected = EpisodeFailureDisposition::Retry {
            delay: Duration::from_secs(5),
            refresh_plan: false,
        };

        assert_eq!(classify("request timed out", 0), expected);
        assert_eq!(classify("connection reset by peer", 1), expected);
        assert_eq!(
            classify("connection reset by peer", 2),
            EpisodeFailureDisposition::FailEntry
        );
    }

    #[test]
    fn recognizes_http_server_errors_and_retry_after() {
        assert_eq!(
            classify("request failed: HTTP 503 Service Unavailable", 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(20),
                refresh_plan: false,
            }
        );

        let error = anyhow!("request failed: status code: 429 Too Many Requests")
            .context("episode transfer failed");
        assert_eq!(
            classify_episode_failure(&error, 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(20),
                refresh_plan: false,
            }
        );
        assert_eq!(
            classify_episode_failure_with_retry_after(&error, 0, Some(Duration::from_secs(47))),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(47),
                refresh_plan: false,
            }
        );
        assert_eq!(
            classify_episode_failure_with_retry_after(&error, 2, Some(Duration::from_secs(47))),
            EpisodeFailureDisposition::FailEntry
        );

        assert_eq!(
            classify("request failed: 503 Service Unavailable", 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(20),
                refresh_plan: false,
            }
        );
    }

    #[test]
    fn generic_403_refreshes_the_plan_without_pausing_the_group() {
        let expected = EpisodeFailureDisposition::Retry {
            delay: Duration::from_secs(5),
            refresh_plan: true,
        };

        assert_eq!(classify("HTTP 403 Forbidden", 0), expected);
        assert_eq!(classify("403 Forbidden", 0), expected);
        assert_eq!(classify("HTTP 403 Forbidden", 1), expected);
        assert_eq!(
            classify("HTTP 403 Forbidden", 2),
            EpisodeFailureDisposition::FailEntry
        );
    }

    #[test]
    fn long_chinese_error_context_does_not_break_http_status_classification() {
        let context = "错误上下文".repeat(16);
        let forbidden = anyhow!("{context} HTTP 403 Forbidden");
        let rate_limited = anyhow!("{context} HTTP 429 Too Many Requests");

        assert_eq!(
            classify_episode_failure(&forbidden, 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(5),
                refresh_plan: true,
            }
        );
        assert_eq!(
            classify_episode_failure(&rate_limited, 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(20),
                refresh_plan: false,
            }
        );
    }

    #[test]
    fn authentication_and_area_restrictions_pause_the_group() {
        for message in [
            "HTTP 401 Unauthorized",
            "Bilibili login expired",
            "credential cookie is invalid",
            "video is unavailable in your region",
            "该视频需要登录",
            "地区限制",
        ] {
            assert_eq!(
                classify(message, 0),
                EpisodeFailureDisposition::PauseGroup,
                "message: {message}"
            );
        }

        assert_eq!(
            classify("HTTP 403 Forbidden", 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(5),
                refresh_plan: true,
            }
        );
    }

    #[test]
    fn shared_storage_and_publication_safety_failures_pause_the_group() {
        for message in [
            "write failed: No space left on device",
            "failed to create output: Read-only file system",
            "unsafe publication: selected file identity changed",
            "output root or staging directory changed during download",
            "Bilibili mux staging directory access path changed: /downloads/.staging",
            "configured output root identity changed: /downloads",
            "failed to validate moved destination through the configured output root",
            "Bilibili group inventory changed since the reviewed plan; inspect the group again",
            "Bilibili group episode numbering changed since the reviewed plan; inspect the group again",
            "Bilibili media requirements changed since the reviewed plan; inspect the group again",
            "saved Bilibili group inventory no longer matches the current selection",
            "Bilibili group entry 17 changed identity since it was saved; inspect the group again",
        ] {
            assert_eq!(
                classify(message, 0),
                EpisodeFailureDisposition::PauseGroup,
                "message: {message}"
            );
        }

        let permission_error = Error::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Permission denied",
        ))
        .context("failed to write output file");
        assert_eq!(
            classify_episode_failure(&permission_error, 0),
            EpisodeFailureDisposition::PauseGroup
        );

        let media_forbidden =
            anyhow!("HTTP 403 Forbidden").context("failed to validate media response");
        assert_eq!(
            classify_episode_failure(&media_forbidden, 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(5),
                refresh_plan: true,
            }
        );

        let remote_permission_denied = anyhow!("HTTP 403 Forbidden: Permission denied")
            .context("failed to validate media response");
        assert_eq!(
            classify_episode_failure(&remote_permission_denied, 0),
            EpisodeFailureDisposition::Retry {
                delay: Duration::from_secs(5),
                refresh_plan: true,
            }
        );
    }

    #[test]
    fn output_recovery_failure_pauses_group_but_episode_failure_stays_local() {
        let recovery_error = anyhow!(
            "video output recovery scan is incomplete; restore access to the reported directories and restart the bot before starting another download"
        )
        .context("failed to acquire the video output guard")
        .context("Bilibili episode 3 download failed");
        assert_eq!(
            classify_episode_failure(&recovery_error, 0),
            EpisodeFailureDisposition::PauseGroup
        );

        let episode_error =
            anyhow!("HTTP 404 Not Found").context("failed to download selected episode 4");
        assert_eq!(
            classify_episode_failure(&episode_error, 0),
            EpisodeFailureDisposition::FailEntry
        );
    }

    #[test]
    fn unavailable_episodes_and_unknown_failures_do_not_retry() {
        for message in [
            "HTTP 404 Not Found",
            "invalid episode id",
            "episode unavailable",
            "unsupported codec in this episode",
        ] {
            assert_eq!(
                classify(message, 0),
                EpisodeFailureDisposition::FailEntry,
                "message: {message}"
            );
        }
    }
}
