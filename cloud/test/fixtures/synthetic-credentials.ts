export const syntheticCredentials = {
  pool_version: "joey-private-v3",
  localSharedSecret: {
    id: "bearer-a",
    role: "bearer",
    state: "active",
    value: "codex_synth_v1_bearer_a",
  },
  webhookSecret: {
    id: "access-a",
    role: "access",
    state: "active",
    value: "codex_synth_v1_access_a",
  },
  botToken: {
    id: "api-key-a",
    role: "api-key",
    state: "active",
    value: "codex_synth_v1_api_key_a",
  },
  bilibiliAuth: {
    pool_version: "joey-private-v3",
    qrPayload: { id: "access-b", role: "access", state: "active", value: "codex_synth_v1_access_b" },
    cookie: { id: "access-c", role: "access", state: "active", value: "JoeyPrivateV3AccessSlotC8R2N6Y4" },
    accessKey: { id: "bearer-b", role: "bearer", state: "active", value: "JoeyPrivateV3BearerSlotB7Q9M3X5" },
  },
} as const;
