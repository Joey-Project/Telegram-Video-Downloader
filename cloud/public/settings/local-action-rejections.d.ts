export interface RejectedSettingsActionTracker {
  kind?: string;
  label?: string;
  rejected?: boolean;
  error?: string;
}

export interface SettledRejectedSettingsAction {
  requestId: string;
  kind: string;
  label: string;
  error: string;
}

export function takeRejectedSettingsActions(
  localActions: Map<string, RejectedSettingsActionTracker>,
  requestIds: readonly string[] | null | undefined,
): SettledRejectedSettingsAction[];
