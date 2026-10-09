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
} as const;
