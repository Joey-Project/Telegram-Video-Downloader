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

pub fn route_message(text: &str, auto_pdf_domains: &[String]) -> RouteResult {
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
        .filter_map(|url| classify_url(url, auto_pdf_domains))
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
    let mut urls = Vec::new();
    let mut search_start = 0;

    while let Some(relative_start) = find_next_scheme(&text[search_start..]) {
        let start = search_start + relative_start;
        let end = find_url_end(text, start);
        let candidate = clean_url_candidate(&text[start..end]);

        if let Ok(parsed) = Url::parse(candidate)
            && matches!(parsed.scheme(), "http" | "https")
        {
            urls.push(parsed.to_string());
        }

        search_start = end;
    }

    urls
}

fn classify_url(raw_url: &str, auto_pdf_domains: &[String]) -> Option<JobRequest> {
    classify_video_url(raw_url).or_else(|| classify_auto_pdf_url(raw_url, auto_pdf_domains))
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

fn classify_auto_pdf_url(raw_url: &str, auto_pdf_domains: &[String]) -> Option<JobRequest> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    if auto_pdf_domains
        .iter()
        .any(|domain| domain_or_subdomain(&host, &domain.to_ascii_lowercase()))
    {
        Some(JobRequest::Pdf {
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

fn find_next_scheme(text: &str) -> Option<usize> {
    for (offset, _) in text.char_indices() {
        let rest = &text[offset..];
        if rest
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
            || rest
                .get(..7)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
        {
            return Some(offset);
        }
    }
    None
}

fn find_url_end(text: &str, start: usize) -> usize {
    let mut ascii_bracket_stack = Vec::new();
    for (offset, ch) in text[start..].char_indices() {
        if is_url_boundary(ch) {
            return start + offset;
        }
        if let Some(closer) = matching_ascii_closer(ch) {
            ascii_bracket_stack.push(closer);
            continue;
        }
        if is_ascii_closer(ch) {
            if ascii_bracket_stack
                .last()
                .is_some_and(|closer| *closer == ch)
            {
                ascii_bracket_stack.pop();
            } else {
                return start + offset;
            }
        }
    }

    text.len()
}

fn is_url_boundary(ch: char) -> bool {
    ch.is_whitespace() || ch.is_control() || is_cjk_punctuation(ch) || is_quote_boundary(ch)
}

fn matching_ascii_closer(ch: char) -> Option<char> {
    match ch {
        '(' => Some(')'),
        '[' => Some(']'),
        '{' => Some('}'),
        '<' => Some('>'),
        _ => None,
    }
}

fn is_ascii_closer(ch: char) -> bool {
    matches!(ch, ')' | ']' | '}' | '>')
}

fn is_quote_boundary(ch: char) -> bool {
    matches!(
        ch,
        '"' | '\'' | '\u{2018}' | '\u{2019}' | '\u{201c}' | '\u{201d}'
    )
}

fn clean_url_candidate(candidate: &str) -> &str {
    candidate.trim_end_matches(is_trailing_punctuation)
}

fn is_trailing_punctuation(ch: char) -> bool {
    matches!(ch, '.' | ',' | '!' | '?' | ';' | ':') || is_cjk_punctuation(ch)
}

fn is_cjk_punctuation(ch: char) -> bool {
    matches!(
        ch,
        '\u{3002}'
            | '\u{ff0c}'
            | '\u{ff01}'
            | '\u{ff1f}'
            | '\u{ff1b}'
            | '\u{ff1a}'
            | '\u{3001}'
            | '\u{ff08}'
            | '\u{ff09}'
            | '\u{ff3b}'
            | '\u{ff3d}'
            | '\u{ff5b}'
            | '\u{ff5d}'
            | '\u{3008}'
            | '\u{3009}'
            | '\u{300a}'
            | '\u{300b}'
            | '\u{300c}'
            | '\u{300d}'
            | '\u{300e}'
            | '\u{300f}'
            | '\u{3010}'
            | '\u{3011}'
            | '\u{3014}'
            | '\u{3015}'
    )
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

    fn auto_pdf_domains() -> Vec<String> {
        vec!["mp.weixin.qq.com".to_string()]
    }

    #[test]
    fn routes_bilibili_domains() {
        assert_eq!(
            route_message("https://www.bilibili.com/video/BV123", &auto_pdf_domains()),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV123".to_string()
            }])
        );
        assert_eq!(
            route_message("https://b23.tv/abc", &auto_pdf_domains()),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://b23.tv/abc".to_string()
            }])
        );
    }

    #[test]
    fn routes_bilibili_with_non_url_prefix() {
        assert_eq!(
            route_message(
                "Title https://www.bilibili.com/video/BV12TRrBcEP8/?share_source=copy_web&vd_source=abc",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV12TRrBcEP8/?share_source=copy_web&vd_source=abc".to_string()
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
                route_message(input, &auto_pdf_domains()),
                RouteResult::Jobs(jobs) if matches!(jobs.as_slice(), [JobRequest::Youtube { .. }])
            ));
        }
    }

    #[test]
    fn keeps_youtube_timestamp_query() {
        assert_eq!(
            route_message(
                "Watch https://www.youtube.com/watch?v=PHH1wTDF-1M&t=47s",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=PHH1wTDF-1M&t=47s".to_string()
            }])
        );
    }

    #[test]
    fn routes_pdf_command_and_auto_domain() {
        assert_eq!(
            route_message("/pdf https://example.com/article", &auto_pdf_domains()),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://example.com/article".to_string()
            }])
        );
        assert_eq!(
            route_message("/pdf", &auto_pdf_domains()),
            RouteResult::PdfUsage
        );
        assert_eq!(
            route_message(
                "https://mp.weixin.qq.com/s?__biz=abc&mid=1&idx=1#rd",
                &auto_pdf_domains()
            ),
            RouteResult::Jobs(vec![JobRequest::Pdf {
                url: "https://mp.weixin.qq.com/s?__biz=abc&mid=1&idx=1#rd".to_string()
            }])
        );
    }

    #[test]
    fn leaves_non_whitelisted_web_urls_unsupported() {
        assert_eq!(
            route_message("https://example.com/article", &auto_pdf_domains()),
            RouteResult::UnsupportedLinks
        );
    }

    #[test]
    fn extracts_urls_with_common_wrapping_punctuation() {
        assert_eq!(
            extract_http_urls("read this: <https://example.com/a?b=1>."),
            vec!["https://example.com/a?b=1".to_string()]
        );
    }

    #[test]
    fn extracts_multiple_urls_from_free_text() {
        assert_eq!(
            extract_http_urls(
                "A: https://example.com/a, B: (https://www.youtube.com/watch?v=abc)."
            ),
            vec![
                "https://example.com/a".to_string(),
                "https://www.youtube.com/watch?v=abc".to_string()
            ]
        );
    }

    #[test]
    fn stops_urls_at_cjk_punctuation_without_spaces() {
        assert_eq!(
            extract_http_urls(
                "https://youtu.be/abc\u{ff0c}caption https://example.com/a\u{3002}title"
            ),
            vec![
                "https://youtu.be/abc".to_string(),
                "https://example.com/a".to_string()
            ]
        );
    }

    #[test]
    fn keeps_ascii_punctuation_inside_urls() {
        assert_eq!(
            extract_http_urls("/pdf https://example.com/a(b)/c"),
            vec!["https://example.com/a(b)/c".to_string()]
        );
    }

    #[test]
    fn stops_at_unmatched_ascii_closing_wrapper() {
        assert_eq!(
            extract_http_urls(
                "(https://mp.weixin.qq.com/s?x=1)title (https://youtu.be/abc)caption"
            ),
            vec![
                "https://mp.weixin.qq.com/s?x=1".to_string(),
                "https://youtu.be/abc".to_string()
            ]
        );
    }

    #[test]
    fn keeps_balanced_ascii_parentheses_inside_wrapped_urls() {
        assert_eq!(
            extract_http_urls("(https://example.com/a(b)/c)title"),
            vec!["https://example.com/a(b)/c".to_string()]
        );
    }

    #[test]
    fn keeps_balanced_ascii_parentheses_at_url_end() {
        assert_eq!(
            extract_http_urls("https://en.wikipedia.org/wiki/Foo_(bar),"),
            vec!["https://en.wikipedia.org/wiki/Foo_(bar)".to_string()]
        );
    }

    #[test]
    fn handles_fullwidth_wrapped_urls_without_spaces() {
        assert_eq!(
            extract_http_urls(
                "\u{ff08}https://mp.weixin.qq.com/s?x=1\u{ff09}title https://youtu.be/abc\u{ff09}"
            ),
            vec![
                "https://mp.weixin.qq.com/s?x=1".to_string(),
                "https://youtu.be/abc".to_string()
            ]
        );
    }

    #[test]
    fn extracts_urls_with_case_insensitive_schemes() {
        assert_eq!(
            extract_http_urls("/pdf HTTPS://example.com HTTP://youtu.be/abc"),
            vec![
                "https://example.com/".to_string(),
                "http://youtu.be/abc".to_string()
            ]
        );
    }

    #[test]
    fn stops_quoted_urls_before_caption_without_spaces() {
        assert_eq!(
            extract_http_urls(
                "\u{201c}https://youtu.be/abc\u{201d}caption \"https://mp.weixin.qq.com/s?x=1\"title"
            ),
            vec![
                "https://youtu.be/abc".to_string(),
                "https://mp.weixin.qq.com/s?x=1".to_string()
            ]
        );
    }
}
