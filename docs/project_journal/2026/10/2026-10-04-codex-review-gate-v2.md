---
id: 20261004-v2gate
title: Codex Review Gate v2 安装
status: active
created: 2026-10-04
updated: 2026-10-04
branch:
pr:
supersedes: []
superseded_by:
---

# Codex Review Gate v2 安装

## 摘要
- 目标分支安装后以 canonical v2 verifier 和 controller 替代专用 v1 review producer，并通过 CODEOWNERS 保护 workflow control plane。

## 当前状态
- `.github/workflows/codex-review-gate.yml` 使用浮动 `@v2` action、只读 verifier 权限（含 `actions: read`）、`request_author_permission: any` 和 `request_review: false`。
- 已安装 `.github/workflows/codex-review-gate-controller.yml`；`.github/CODEOWNERS` 将 workflow 与自身的所有权指定为 `@JoeyTeng`。
- 此 consumer 安装不修改 production ruleset；本记录不表示 v2 production requirement 或更广的仓库迁移已完成。

## 后续步骤
- 在保留既有规则且不新增 `@codex` requirement 的前提下，完成另行授权的 production-ruleset 转换；其余 consumer rollout 完成前，不将共享切换记录为已完成。

## 证据
- Consumer implementation commit：`4dafebfec41a99891bb5fda8bd561a9f1bd949bb`。
- Canonical bootstrap preview/apply 与 verifier/controller 模板逐字节比较通过；`actionlint` 1.7.12 和 `git diff --check` 通过。
