//! Time-aware placement for v2 ASS output.
//!
//! Score each candidate by the estimated area and duration of collisions.
//! Each prior event adds its horizontal overlap integral across the rows it
//! occupies. Integrating the result gives a constant-time score for any
//! candidate's vertical span, so a placement costs O(active occupancy + height).
//! The occupancy window is deliberately capped: at most 256 prior events are
//! considered, even for 50,000 comments at one timestamp. This makes the
//! allocator's worst-case work O(events * (256 + canvas height)) and gives
//! deterministic behavior in saturated scenes.

use std::collections::VecDeque;
use std::ops::{Range, RangeInclusive};

use super::{
    Event, PLAY_RES_X, RenderOptions, TOP_MARGIN, count_growth, estimated_text_width, event_times,
};

const MAX_ACTIVE_OCCUPANCY: usize = 256;
const CONFLICT_WEIGHT_SCALE: f64 = 1_000.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Placement {
    /// ASS y anchor, rounded to a pixel. Bottom-aligned events use their lower
    /// edge as the anchor; all other modes use the upper edge.
    pub anchor_y: i32,
}

#[derive(Debug, Clone, Copy)]
struct Motion {
    start_cs: u64,
    end_cs: u64,
    x_start: f64,
    x_end: f64,
    width: f64,
}

impl Motion {
    fn x_at(self, timestamp_cs: u64) -> f64 {
        let duration = self.end_cs - self.start_cs;
        let progress = (timestamp_cs - self.start_cs) as f64 / duration as f64;
        self.x_start + (self.x_end - self.x_start) * progress
    }

    fn velocity(self) -> f64 {
        (self.x_end - self.x_start) / (self.end_cs - self.start_cs) as f64
    }
}

#[derive(Debug, Clone)]
struct Occupied {
    motion: Motion,
    visible_rows: Range<usize>,
}

/// Size one v2 event by its longest rendered line. A grouped event's counter
/// suffix is included on the final line, matching the emitted ASS text.
pub(super) fn size_event_v2(event: &mut Event<'_>, options: &RenderOptions) {
    event.body_font_size =
        (event.anchor.font_size * options.font_scale * count_growth(event.count))
            .round()
            .max(1.0);
    event.counter_font_size = (event.body_font_size * 0.6).round().max(1.0);

    let suffix = (event.count > 1).then(|| format!("({})", event.count));
    let mut lines = event.anchor.text.split('\n').peekable();
    let mut width: f64 = 0.0;
    while let Some(line) = lines.next() {
        let mut line_width = estimated_text_width(line, event.body_font_size);
        if lines.peek().is_none()
            && let Some(suffix) = &suffix
        {
            line_width +=
                estimated_text_width(suffix, event.counter_font_size) + event.body_font_size * 0.3;
        }
        width = width.max(line_width);
    }
    event.width = width.round_ties_even();

    let line_count = event.anchor.text.matches('\n').count() + 1;
    event.height = event.body_font_size * 1.25 * line_count as f64 + 4.0;
}

pub(super) fn place_events(events: &[Event<'_>], options: &RenderOptions) -> Vec<Placement> {
    let region_height = (f64::from(super::PLAY_RES_Y) * options.display_area)
        .floor()
        .max(1.0) as usize;
    let margin = (TOP_MARGIN as usize).min(region_height / 8);
    let mut active: VecDeque<Occupied> = VecDeque::with_capacity(MAX_ACTIVE_OCCUPANCY);
    let mut placements = Vec::with_capacity(events.len());
    let mut row_costs = vec![0_i64; region_height + 1];

    for event in events {
        let (start_cs, end_cs) = event_times(event);
        while active
            .front()
            .is_some_and(|occupied| occupied.motion.end_cs <= start_cs)
        {
            active.pop_front();
        }

        let height = event.height.ceil().min(region_height as f64) as usize;
        let bottom_aligned = event.anchor.mode == 4;
        let candidates = candidate_anchors(height, region_height, margin, bottom_aligned);
        let current_motion = motion_for(event, start_cs, end_cs);
        let anchor_y = choose_candidate(
            candidates,
            &active,
            &mut row_costs,
            current_motion,
            height,
            region_height,
            bottom_aligned,
        );
        placements.push(Placement {
            anchor_y: anchor_y as i32,
        });

        if end_cs > start_cs {
            if active.len() == MAX_ACTIVE_OCCUPANCY {
                active.pop_front();
            }
            active.push_back(Occupied {
                motion: current_motion,
                visible_rows: visible_rows(anchor_y, height, region_height, bottom_aligned),
            });
        }
    }

    placements
}

fn candidate_anchors(
    height: usize,
    region_height: usize,
    margin: usize,
    bottom_aligned: bool,
) -> RangeInclusive<usize> {
    let bottom = region_height - margin;
    if bottom_aligned {
        (margin + height).min(bottom)..=bottom
    } else {
        margin..=bottom.saturating_sub(height).max(margin)
    }
}

fn visible_rows(
    anchor_y: usize,
    height: usize,
    region_height: usize,
    bottom_aligned: bool,
) -> Range<usize> {
    if bottom_aligned {
        anchor_y.saturating_sub(height)..anchor_y
    } else {
        anchor_y..(anchor_y + height).min(region_height)
    }
}

fn motion_for(event: &Event<'_>, start_cs: u64, end_cs: u64) -> Motion {
    let width = event.width;
    let (x_start, x_end) = match event.anchor.mode {
        4 | 5 => {
            let center = f64::from(PLAY_RES_X) / 2.0;
            (center - width / 2.0, center - width / 2.0)
        }
        6 => (-width, f64::from(PLAY_RES_X) + width),
        _ => (f64::from(PLAY_RES_X) + width, -width),
    };
    Motion {
        start_cs,
        end_cs,
        x_start,
        x_end,
        width,
    }
}

fn horizontal_overlap(left: Motion, right: Motion) -> Option<f64> {
    let start = left.start_cs.max(right.start_cs);
    let end = left.end_cs.min(right.end_cs);
    if start >= end {
        return None;
    }

    let left_start_x = left.x_at(start);
    let right_start_x = right.x_at(start);
    let left_velocity = left.velocity();
    let right_velocity = right.velocity();
    let horizon = (end - start) as f64;
    let mut minimum_delta = 0.0;
    let mut maximum_delta = horizon;

    // Require both boxes to intersect the clipped playfield as well as each
    // other; this excludes collisions that happen only before entry or after
    // exit. Each condition is linear in time, so their common interval is exact
    // for these constant-speed ASS paths.
    let constraints = [
        (
            left_start_x - right_start_x - right.width,
            left_velocity - right_velocity,
        ),
        (
            right_start_x - left_start_x - left.width,
            right_velocity - left_velocity,
        ),
        (left_start_x - f64::from(PLAY_RES_X), left_velocity),
        (-left_start_x - left.width, -left_velocity),
        (right_start_x - f64::from(PLAY_RES_X), right_velocity),
        (-right_start_x - right.width, -right_velocity),
    ];
    for (value, slope) in constraints {
        if !constrain_nonpositive(value, slope, &mut minimum_delta, &mut maximum_delta) {
            return None;
        }
    }

    let duration = maximum_delta - minimum_delta;
    if duration <= 0.0 {
        return None;
    }

    // Integrate the overlap width over this interval by sampling the midpoint
    // of each third. This distinguishes a brief edge touch from two mostly
    // superimposed lines without enumerating frames or allocating per pair.
    let mut overlap_width_sum = 0.0;
    for fraction in [1.0 / 6.0, 0.5, 5.0 / 6.0] {
        let delta = minimum_delta + fraction * duration;
        let left_x = left_start_x + left_velocity * delta;
        let right_x = right_start_x + right_velocity * delta;
        let overlap_start = left_x.max(right_x).max(0.0);
        let overlap_end = (left_x + left.width)
            .min(right_x + right.width)
            .min(f64::from(PLAY_RES_X));
        overlap_width_sum += (overlap_end - overlap_start).max(0.0);
    }
    Some(overlap_width_sum * duration / 300.0)
}

fn constrain_nonpositive(
    value_at_start: f64,
    slope: f64,
    minimum: &mut f64,
    maximum: &mut f64,
) -> bool {
    if slope == 0.0 {
        return value_at_start < 0.0;
    }
    let crossing = -value_at_start / slope;
    if slope > 0.0 {
        *maximum = maximum.min(crossing);
    } else {
        *minimum = minimum.max(crossing);
    }
    *minimum <= *maximum
}

fn choose_candidate(
    candidates: RangeInclusive<usize>,
    active: &VecDeque<Occupied>,
    row_costs: &mut [i64],
    current_motion: Motion,
    height: usize,
    region_height: usize,
    bottom_aligned: bool,
) -> usize {
    let preferred_y = if bottom_aligned {
        *candidates.end()
    } else {
        *candidates.start()
    };
    if candidates.start() == candidates.end() || active.is_empty() {
        return preferred_y;
    }

    row_costs.fill(0);
    for previous in active {
        if let Some(overlap) = horizontal_overlap(current_motion, previous.motion) {
            let weight = (overlap * CONFLICT_WEIGHT_SCALE).ceil() as i64;
            row_costs[previous.visible_rows.start] += weight;
            row_costs[previous.visible_rows.end] -= weight;
        }
    }

    // Convert range updates to row weights and then to a prefix integral.
    let mut row_weight = 0;
    let mut area = 0;
    for cost in row_costs.iter_mut() {
        row_weight += *cost;
        *cost = area;
        area += row_weight;
    }

    let mut best_y = preferred_y;
    let mut best_score = i64::MAX;
    for y in candidates {
        let rows = visible_rows(y, height, region_height, bottom_aligned);
        let score = row_costs[rows.end] - row_costs[rows.start];
        if score < best_score || (score == best_score && bottom_aligned && y > best_y) {
            best_score = score;
            best_y = y;
        }
    }
    best_y
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_ACTIVE_OCCUPANCY, Motion, Occupied, choose_candidate, horizontal_overlap, place_events,
        size_event_v2,
    };
    use crate::{Comment, RenderOptions, make_event};
    use std::collections::VecDeque;

    #[test]
    fn linear_motion_intersection_covers_opposing_and_fixed_boxes() {
        let right_to_left = Motion {
            start_cs: 100,
            end_cs: 900,
            x_start: 1_940.0,
            x_end: -20.0,
            width: 20.0,
        };
        let left_to_right = Motion {
            start_cs: 100,
            end_cs: 900,
            x_start: -40.0,
            x_end: 1_960.0,
            width: 40.0,
        };
        let fixed = Motion {
            start_cs: 100,
            end_cs: 500,
            x_start: 900.0,
            x_end: 900.0,
            width: 120.0,
        };

        assert!(horizontal_overlap(right_to_left, left_to_right).is_some());
        assert!(horizontal_overlap(right_to_left, fixed).is_some());
        assert!(
            horizontal_overlap(
                Motion {
                    start_cs: 100,
                    end_cs: 900,
                    x_start: 2_300.0,
                    x_end: 2_100.0,
                    width: 100.0,
                },
                Motion {
                    start_cs: 100,
                    end_cs: 500,
                    x_start: 2_200.0,
                    x_end: 2_200.0,
                    width: 100.0,
                }
            )
            .is_none()
        );
        assert!(
            horizontal_overlap(
                right_to_left,
                Motion {
                    start_cs: 900,
                    end_cs: 1_700,
                    x_start: 1_940.0,
                    x_end: -20.0,
                    width: 20.0,
                }
            )
            .is_none()
        );
    }

    #[test]
    fn horizontal_overlap_counts_only_the_shared_on_screen_area() {
        let wide = Motion {
            start_cs: 0,
            end_cs: 400,
            x_start: -10.0,
            x_end: -10.0,
            width: 100.0,
        };
        let narrow = Motion {
            width: 40.0,
            ..wide
        };
        assert_eq!(horizontal_overlap(wide, narrow), Some(120.0));
    }

    #[test]
    fn saturated_layout_prefers_less_vertical_overlap() {
        let motion = Motion {
            start_cs: 0,
            end_cs: 400,
            x_start: 100.0,
            x_end: 100.0,
            width: 100.0,
        };
        let active = VecDeque::from([Occupied {
            motion,
            visible_rows: 10..70,
        }]);
        let mut scores = [0; 101];
        assert_eq!(
            choose_candidate(0..=40, &active, &mut scores, motion, 60, 100, false),
            40
        );
    }

    #[test]
    fn saturated_same_timestamp_placement_is_bounded_and_deterministic() {
        let comments = (0..MAX_ACTIVE_OCCUPANCY + 32)
            .map(|index| Comment {
                original_index: index,
                start_seconds: 1.0,
                mode: 1,
                font_size: 25.0,
                color: 0xFF_FF_FF,
                text: format!("event-{index:04}"),
            })
            .collect::<Vec<_>>();
        let options = RenderOptions::default();
        let mut events = comments.iter().map(make_event).collect::<Vec<_>>();
        for event in &mut events {
            size_event_v2(event, &options);
        }

        let first = place_events(&events, &options);
        let second = place_events(&events, &options);
        assert_eq!(first, second);
        assert_eq!(first.len(), comments.len());
        assert!(
            first
                .iter()
                .all(|placement| (40..=1_040).contains(&placement.anchor_y))
        );
    }
}
