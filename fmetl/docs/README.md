# FM ETL 文档索引

> 翠花当家 v0.10 ETL 管道的全部文档导航。

---

## 文档结构

```
docs/
├── README.md                           (本文件 — 文档导航)
├── architecture/                       (架构与处理逻辑)
│   └── ETL_v0.10_完整处理逻辑.md
├── references/                         (参考手册)
│   ├── strategy_fm_字段手册_完整版.md   (源表字段手册 — 唯一权威完整版)
│   └── strategy_fm_字段手册_BOM版.md   (BOM 专用字段手册)
├── reviews/                            (审查与对比报告)
│   ├── 全面审查报告_v0.10_2026-06-01.md
│   └── 差异问题与待办事项_v0.10.md
└── fixes/                              (修复记录)
    ├── README.md                       (修复索引 + 依赖关系图)
    ├── FIX-001 ~ FIX-013
```

---

## 各目录说明

### architecture/ — 架构文档

| 文件 | 内容 |
|------|------|
| [ETL_v0.10_完整处理逻辑.md](architecture/ETL_v0.10_完整处理逻辑.md) | 价格体系 + 13步详解 + 分类重映射 + 字段流转 |

### references/ — 参考手册

| 文件 | 说明 |
|------|------|
| [strategy_fm_字段手册_完整版.md](references/strategy_fm_字段手册_完整版.md) | **唯一权威**源表字段手册，覆盖全部 21 张源表 |
| [strategy_fm_字段手册_BOM版.md](references/strategy_fm_字段手册_BOM版.md) | BOM 分摊专用字段手册，按父子品粒度展开 |

> 注意：`strategy_fm_字段手册_v4.md` 已删除（内容被完整版覆盖）。

### reviews/ — 审查报告

| 文件 | 说明 |
|------|------|
| [全面审查报告_v0.10_2026-06-01.md](reviews/全面审查报告_v0.10_2026-06-01.md) | 2026年6月全系统审查，含 4 个核心问题的根因分析 |
| [差异问题与待办事项_v0.10.md](reviews/差异问题与待办事项_v0.10.md) | QDM vs FM 差异追踪 + 行动计划 |

### fixes/ — 修复记录

| 文件 | 说明 |
|------|------|
| [fixes/README.md](fixes/README.md) | 修复索引 + 依赖关系图 + 实现顺序 |
| FIX-001 ~ FIX-013 | 每条修复的详细分析 |

---

## 其他文档入口

| 文档 | 位置 | 说明 |
|------|------|------|
| CLAUDE.md | `../CLAUDE.md` | 项目级 AI 操作手册（给 Claude Code 看） |
| fmetl/README.md | `../README.md` | 项目总览 + 快速开始（给人看） |
| atomic/README.md | `../atomic/README.md` | 原子域提取器字段手册 |
| calculated/README.md | `../calculated/README.md` | 计算层算法详解 |
| connectors/README.md | `../connectors/README.md` | API 连接器使用说明 |
| fm_tables/README.md | `../fm_tables/README.md` | FM 底表产出说明 |
| utils/README.md | `../utils/README.md` | 工具模块说明 |

---

## 文档维护约定

1. **字段手册**: `references/strategy_fm_字段手册_完整版.md` 是唯一权威版本。字段变更时更新它，不要创建新版本文件。
2. **修复记录**: 每次代码修复写一个 FIX-NNN.md，更新 `fixes/README.md` 索引和依赖关系图。
3. **审查报告**: 每次全系统审查写一个带日期的报告，放入 `reviews/`。
4. **README vs CLAUDE.md**: README 是给人看的详细文档（保留所有细节），CLAUDE.md 是给 AI 看的操作手册（核心规则 + 快速索引）。
