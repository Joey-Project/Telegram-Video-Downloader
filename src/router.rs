use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobRequest {
    Bilibili { url: String },
    Youtube { url: String },
    Pdf { url: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteResult {
    Jobs(Vec<JobRequest>),
    PdfUsage,
    UnsupportedLinks,
    Empty,
}

impl JobRequest {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Bilibili { .. } => "Bilibili download",
            Self::Youtube { .. } => "YouTube download",
            Self::Pdf { .. } => "PDF capture",
        }
    }
}

pub fn route_message(text: &str) -> RouteResult {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return RouteResult::Empty;
    }

    if let Some(pdf_args) = pdf_command_args(trimmed) {
        let jobs: Vec<_> = extract_http_urls(pdf_args)
            .into_iter()
            .map(|url| JobRequest::Pdf { url })
            .collect();
        return if jobs.is_empty() {
            RouteResult::PdfUsage
        } else {
            RouteResult::Jobs(jobs)
        };
    }

    let urls = extract_http_urls(trimmed);
    let jobs: Vec<_> = urls
        .iter()
        .filter_map(|url| classify_video_url(url))
        .collect();

    if !jobs.is_empty() {
        RouteResult::Jobs(jobs)
    } else if urls.is_empty() {
        RouteResult::Empty
    } else {
        RouteResult::UnsupportedLinks
    }
}

pub fn extract_http_urls(text: &str) -> Vec<String> {
    text.split_whitespace()
        .filter_map(|token| {
            let candidate = clean_url_token(token);
            let parsed = Url::parse(candidate).ok()?;
            match parsed.scheme() {
                "http" | "https" => Some(parsed.to_string()),
                _ => None,
            }
        })
        .collect()
}

fn classify_video_url(raw_url: &str) -> Option<JobRequest> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();

    if host == "b23.tv" || domain_or_subdomain(&host, "bilibili.com") {
        Some(JobRequest::Bilibili {
            url: raw_url.to_string(),
        })
    } else if is_youtube_host(&host) {
        Some(JobRequest::Youtube {
            url: raw_url.to_string(),
        })
    } else {
        None
    }
}

fn pdf_command_args(text: &str) -> Option<&str> {
    let first = text.split_whitespace().next()?;
    if first == "/pdf" || first.starts_with("/pdf@") {
        Some(text[first.len()..].trim())
    } else {
        None
    }
}

fn clean_url_token(token: &str) -> &str {
    let token = token
        .trim_matches(|ch| {
            matches!(
                ch,
                '<' | '>' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\''
            )
        })
        .trim_end_matches(['.', ',', '!', '?', ';']);

    token.trim_matches(|ch| {
        matches!(
            ch,
            '<' | '>' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\''
        )
    })
}

fn domain_or_subdomain(host: &str, domain: &str) -> bool {
    host == domain
        || host
            .strip_suffix(domain)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn is_youtube_host(host: &str) -> bool {
    host == "youtu.be"
        || domain_or_subdomain(host, "youtube.com")
        || domain_or_subdomain(host, "youtube-nocookie.com")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_bilibili_domains() {
        assert_eq!(
            route_message("https://www.bilibili.com/video/BV123"),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV123".to_string()
            }])
        );
        assert_eq!(
            route_message("https://b23.tv/abc"),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://b23.tv/abc".to_string()
            }])
        );
    }

    #[test]
    fn routes_youtube_domains() {
        for input in [
            "https://youtube.com/watch?v=abc",
            "https://www.youtube.com/watch?v=abc",
            "https://m.youtube.com/watch?v=abc",
            "https://music.youtube.com/watch?v=abc",
            "https://youtu.be/abc",
            "https://www.youtube-nocookie.com/embed/abc",
        ] {
            assert!(matches!(
                route_message(input),
                RouteResult::Jobs(jobs) if matches!(jobs.as_slice(), [JobRequest::Youtube { .. }])
            ));
        }
    }

    #[test]
    fn requires_explicit_pdf_command() {
        assert_eq!(
            route_message("/pdf https://example.com/article"),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://example.com/article".to_string()
            }])
        );
        assert_eq!(
            route_message("https://example.com/article"),
            RouteResult::UnsupportedLinks
        );
        assert_eq!(route_message("/pdf"), RouteResult::PdfUsage);
    }

    #[test]
    fn extracts_urls_with_common_wrapping_punctuation() {
        assert_eq!(
            extract_http_urls("read this: <https://example.com/a?b=1>."),
            vec!["https://example.com/a?b=1".to_string()]
        );
    }
}
