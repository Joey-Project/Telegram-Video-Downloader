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

function mergedLibraryDescriptors(snapshot) {
  const library = snapshot?.library;
  const applied = Array.isArray(snapshot?.settings?.applied?.libraries)
    ? snapshot.settings.applied.libraries
    : [];
  const live = Array.isArray(library?.libraries) ? library.libraries : [];
  const byId = new Map();
  for (const entry of applied) {
    if (entry && typeof entry.id === "string") byId.set(entry.id, { ...entry });
  }
  for (const entry of live) {
    if (entry && typeof entry.id === "string") {
      byId.set(entry.id, { ...byId.get(entry.id), ...entry });
    }
  }
  return byId;
}

function retainLibrarySnapshots(previousSnapshot, incomingSnapshot) {
  const nextLibraries = incomingSnapshot?.library?.libraries;
  if (!Array.isArray(nextLibraries)) return incomingSnapshot;

  const previous = mergedLibraryDescriptors(previousSnapshot);
  const incoming = mergedLibraryDescriptors(incomingSnapshot);
  const previousDefaultId = previousSnapshot?.settings?.applied?.default_library_id
    ?? previousSnapshot?.library?.default_library_id;
  const libraries = nextLibraries.map((entry) => {
    if (!entry || typeof entry.id !== "string") return entry;
    const current = incoming.get(entry.id);
    const prior = previous.get(entry.id);
    if (!current || !prior || typeof current.root !== "string" || !current.root
      || current.root !== prior.root) return entry;

    let priorSnapshot = prior.snapshot;
    if ((!priorSnapshot || typeof priorSnapshot !== "object")
      && previousDefaultId === entry.id && previousSnapshot?.library) {
      priorSnapshot = previousSnapshot.library;
    }
    if (!priorSnapshot || typeof priorSnapshot !== "object"
      || !Array.isArray(priorSnapshot.items) || priorSnapshot.ready === false) return entry;

    const currentSnapshot = current.snapshot;
    const snapshotReady = currentSnapshot && typeof currentSnapshot === "object"
      && currentSnapshot.ready !== false && Array.isArray(currentSnapshot.items);
    const availability = String(current.availability ?? "").toLowerCase();
    const unavailable = ["offline", "unavailable"].includes(availability) || current.available === false;
    if (snapshotReady && !unavailable) {
      const { snapshot_stale: _snapshotStale, ...freshEntry } = entry;
      return freshEntry;
    }

    const cachedSnapshot = {
      ...priorSnapshot,
      stale: true,
      items: priorSnapshot.items.map((item) => ({ ...item, presence: "unavailable" })),
    };
    return { ...entry, snapshot: cachedSnapshot, snapshot_stale: true };
  });
  return {
    ...incomingSnapshot,
    library: { ...incomingSnapshot.library, libraries },
  };
}

export function acceptOwnerSnapshot(snapshot, previousSession = null) {
  const previousSnapshot = previousSession?.ownerVerified === true
    ? previousSession.snapshot
    : null;
  return {
    ownerVerified: true,
    snapshot: retainLibrarySnapshots(previousSnapshot, snapshot),
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

export async function drainActionsAfterSnapshotRefresh(session, online, drain) {
  if (online !== true || !mayDispatchActions(session)) return false;
  return await drain();
}
