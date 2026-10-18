export interface OwnerSnapshotSession<T = unknown> {
  ownerVerified: boolean;
  snapshot: T | null;
  privateUiVisible: boolean;
  canDispatchActions: boolean;
  clearCache: boolean;
  authorizationRejected: boolean;
}

export function beginOwnerVerification(): OwnerSnapshotSession<never>;
export function acceptOwnerSnapshot<T>(snapshot: T): OwnerSnapshotSession<T>;
export function failOwnerSnapshot<T>(session: OwnerSnapshotSession<T>, status: number | undefined, now?: number): OwnerSnapshotSession<T>;
export function mayUsePrivateSnapshot(session: OwnerSnapshotSession<unknown>): boolean;
export function mayDispatchActions(session: OwnerSnapshotSession<unknown>): boolean;
