export function beginOwnerVerification() {
  return {
    ownerVerified: false,
    snapshot: null,
    privateUiVisible: false,
    canDispatchActions: false,
    clearCache: true,
    authorizationRejected: false,
  };
}

export function acceptOwnerSnapshot(snapshot) {
  return {
    ownerVerified: true,
    snapshot,
    privateUiVisible: true,
    canDispatchActions: true,
    clearCache: false,
    authorizationRejected: false,
  };
}

export function failOwnerSnapshot(session, status, now = Date.now()) {
  if (status === 401 || status === 403) {
    return {
      ownerVerified: false,
      snapshot: null,
      privateUiVisible: false,
      canDispatchActions: false,
      clearCache: true,
      authorizationRejected: true,
    };
  }
  if (session?.ownerVerified === true && session.snapshot) {
    return {
      ownerVerified: true,
      snapshot: { ...session.snapshot, stale: true, cached_at: now },
      privateUiVisible: true,
      canDispatchActions: true,
      clearCache: false,
      authorizationRejected: false,
    };
  }
  return {
    ownerVerified: false,
    snapshot: null,
    privateUiVisible: false,
    canDispatchActions: false,
    clearCache: false,
    authorizationRejected: false,
  };
}

export function mayUsePrivateSnapshot(session) {
  return session?.ownerVerified === true
    && session?.privateUiVisible === true
    && session.snapshot !== null
    && session.snapshot !== undefined;
}

export function mayDispatchActions(session) {
  return session?.ownerVerified === true && session?.canDispatchActions === true;
}
