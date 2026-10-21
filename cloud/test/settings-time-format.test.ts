import { describe, expect, it } from "vitest";
import { timeAgoMilliseconds, timeAgoSeconds } from "../public/settings/time-format.js";

describe("settings relative timestamp formatting", () => {
  const now = Date.UTC(2026, 9, 21, 12, 0, 0);

  it("formats Worker epoch milliseconds without treating them as seconds", () => {
    const workerLastSeen = now - 12 * 60_000;
    const pendingRequestCreatedAt = now - 3 * 60_000;

    expect(timeAgoMilliseconds(workerLastSeen, now)).toBe("12 min ago");
    expect(timeAgoMilliseconds(pendingRequestCreatedAt, now)).toBe("3 min ago");
  });

  it("formats local operation epoch seconds explicitly", () => {
    const operationUpdatedAt = Math.floor(now / 1_000) - 2 * 3_600;

    expect(timeAgoSeconds(operationUpdatedAt, now)).toBe("2 hr ago");
  });

  it("keeps invalid timestamps unavailable and current timestamps recent", () => {
    expect(timeAgoMilliseconds(null, now)).toBe("Not yet");
    expect(timeAgoSeconds(Number.NaN, now)).toBe("Not yet");
    expect(timeAgoMilliseconds(now, now)).toBe("Just now");
  });
});
