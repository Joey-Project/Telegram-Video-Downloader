---
id: 20261121-dmr01
title: 迁移分组弹幕渲染依赖
status: completed
created: 2026-11-21
updated: 2026-11-21
branch:
pr:
supersedes: []
superseded_by:
---

# 迁移分组弹幕渲染依赖

## Summary

- Bot 使用 crates.io 上的 `danmaku-renderer`，按现有样式设置生成 v4 ASS。已有 v1–v3 ASS 仍按原始布局和设置核对，手工编辑保护留在应用内。

## Validation

- 覆盖旧版本前缀识别、已有 sidecar 升级、下载及弹幕刷新流程。

## Evidence

- [danmaku-renderer 0.1.0](https://crates.io/crates/danmaku-renderer/0.1.0)
