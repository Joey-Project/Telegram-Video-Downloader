use anyhow::{Result, bail};
use unicode_segmentation::UnicodeSegmentation;

/// Builds the standard media stem while reserving all non-title filename data.
/// `stem_budget` excludes the extension, matched attachment suffixes, and any
/// KeepBoth suffix, which callers must reserve before calling this function.
pub(crate) fn standard_media_stem(
    title: &str,
    collection_order: Option<u32>,
    source_id: Option<&str>,
    height: Option<u32>,
    codec: Option<&str>,
    stem_budget: usize,
    keep_both_suffix: &str,
) -> Result<String> {
    let mut prefix = Vec::new();
    if let Some(order) = collection_order.filter(|order| *order > 0) {
        prefix.push(format!("P{order:03}"));
    }

    let mut suffix = Vec::new();
    if let Some(source_id) = source_id
        .map(str::trim)
        .filter(|source_id| !source_id.is_empty())
    {
        let sanitized_source_id = sanitize_fixed(source_id);
        if !sanitized_source_id.is_empty() {
            suffix.push(format!("[{sanitized_source_id}]"));
        }
    }
    let mut specs = Vec::new();
    if let Some(height) = height.filter(|height| *height > 0) {
        specs.push(format!("{height}p"));
    }
    if let Some(codec) = codec.map(str::trim).filter(|codec| !codec.is_empty()) {
        specs.push(sanitize_fixed(&codec.to_ascii_lowercase()));
    }
    if !specs.is_empty() {
        suffix.push(format!("[{}]", specs.join(" ")));
    }

    let prefix = prefix.join(" - ");
    let suffix = suffix.join(" - ");
    let separator_bytes = usize::from(!prefix.is_empty()) * 3 + usize::from(!suffix.is_empty()) * 3;
    let fixed_bytes = prefix.len() + suffix.len() + separator_bytes;
    let title_budget = stem_budget.checked_sub(fixed_bytes).ok_or_else(|| {
        anyhow::anyhow!("standard media identity fields exceed the available filename budget")
    })?;

    let title = sanitize_title(title);
    let fitted_title = fit_title(&title, title_budget)?;

    let mut pieces = Vec::with_capacity(3);
    if !prefix.is_empty() {
        pieces.push(prefix);
    }
    pieces.push(fitted_title);
    if !suffix.is_empty() {
        pieces.push(suffix);
    }
    let mut stem = pieces.join(" - ");
    stem.push_str(keep_both_suffix);
    Ok(stem)
}

fn sanitize_fixed(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for character in value.chars() {
        result.push(
            if character.is_control() || matches!(character, '/' | '\\' | ':' | '\0') {
                '_'
            } else {
                character
            },
        );
    }
    let mut result = result.trim().trim_matches('.').to_string();
    while result.contains("..") {
        result = result.replace("..", ".");
    }
    result
}

fn sanitize_title(value: &str) -> String {
    let title = sanitize_fixed(value);
    if title.is_empty() {
        "Untitled".to_string()
    } else {
        title
    }
}

fn fit_title(title: &str, budget: usize) -> Result<String> {
    if title.len() <= budget {
        return Ok(title.to_string());
    }

    const ELLIPSIS: &str = "…";
    let prefix_budget = budget.checked_sub(ELLIPSIS.len()).ok_or_else(|| {
        anyhow::anyhow!("standard media identity fields leave no room for a shortened title")
    })?;
    let mut fitted = String::new();
    for grapheme in title.graphemes(true) {
        if fitted.len() + grapheme.len() > prefix_budget {
            break;
        }
        fitted.push_str(grapheme);
    }
    fitted.push_str(ELLIPSIS);
    if fitted.len() > budget {
        bail!("shortened title exceeds the available filename budget");
    }
    Ok(fitted)
}

#[cfg(test)]
mod tests {
    use super::standard_media_stem;

    #[test]
    fn reserves_order_source_and_specs_while_shortening_multilingual_titles() {
        let combining_title = "e\u{301}".repeat(12);
        for title in [
            "界界界界界界界界界界界界界界界界",
            "👩‍👩‍👧‍👦👩‍👩‍👧‍👦👩‍👩‍👧‍👦",
            combining_title.as_str(),
            "Русское название Русское название",
            "Sous-titres français Sottotitoli italiani",
        ] {
            let stem = standard_media_stem(
                title,
                Some(1),
                Some("BV1identity"),
                Some(1080),
                Some("h264"),
                52,
                "",
            )
            .expect("title should fit around reserved identity fields");
            assert!(stem.starts_with("P001 - "));
            assert!(stem.ends_with("[BV1identity] - [1080p h264]"));
            assert!(stem.len() <= 52);
            assert!(stem.contains('…'));
        }
    }

    #[test]
    fn pads_order_to_three_digits_without_truncating_larger_orders() {
        for (order, expected) in [(1, "P001"), (12, "P012"), (203, "P203"), (1000, "P1000")] {
            let stem = standard_media_stem("Title", Some(order), None, None, None, 100, "")
                .expect("order should fit");
            assert!(stem.starts_with(expected));
        }
        assert_eq!(
            standard_media_stem("Standalone", None, None, None, None, 100, "").unwrap(),
            "Standalone"
        );
    }

    #[test]
    fn reports_fixed_fields_that_cannot_fit_without_dropping_identity() {
        let error = standard_media_stem(
            "Title",
            Some(1),
            Some("very-long-id"),
            Some(1080),
            Some("h264"),
            20,
            "",
        )
        .expect_err("fixed identity should not be truncated");
        assert!(error.to_string().contains("identity fields"));
    }
}
