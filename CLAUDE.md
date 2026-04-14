# Project Intelligence & Operational Logic

This file is the operational core for Claude. Gemini CLI and Claude MUST follow these protocols to maintain project integrity.

## 🎯 Architectural Intent
- **Core Mission:** The Time-Machine for Intelligence: The global infrastructure for AI to learn from history in milliseconds.
- **Primary Stack:** Rust (Tokio/io_uring), Custom LSM-Tree, FlatBuffers (Zero-copy), SIMD (AVX-512)
- **System Nature:** A specialized time-series storage designed to capture every evolution of the knowledge graph in real-time, serving as the permanent memory layer for Chronos.

## 🧬 Automated Lifecycle Management
1. **Research Sync:** When `./scripts/update_notebookLM.sh` is executed:
   - You MUST update `DESIGN_DECISIONS.md` with new ADRs found in research.
   - **Constraint:** Maintain a rolling log of the **latest 10 ADRs**.
2. **PR Creation Protocol:** When instructed to create a Pull Request:
   - **Summarize:** Analyze all commit messages since the last merge to `main`.
   - **Template:** Read `.github/PULL_REQUEST_TEMPLATE.md` and populate it with:
     - Detailed description of changes.
     - Linked Issue ID (search for keywords like "fixes #123").
     - Automated Labels (e.g., `feat`, `fix`, `docs`).
   - **Assign:** Automatically set the current developer as the Assignee.
3. **Pre-Commit Action:** Before every commit, you MUST:
   - Run `tree -I 'node_modules|.git|target' > STRUCTURE.tree`.
   - Trigger stack-specific formatting (e.g., `cargo fmt`).
   - Run `pre-commit run --all-files` if available.

## 🛠️ Tooling & Standards
- **Translation:** All technical specifications are English. `locales/` MUST be kept in sync and translated for users documentation.
- **Workflow Mastery:** Use `/superpower:executing-plans` for feature work.
- **Automation:** Refer to `.github/workflows/pr_automation.yml` for server-side PR handling.

## 📂 Template Inventory
You manage: ARCHITECTURE.md, ROADMAP.md, CONTRIBUTING.md, DESIGN_DECISIONS.md, STRUCTURE.tree, SECURITY.md, LICENSE.md, FAQ.md, GOVERNANCE.md, SUPPORT.md, TROUBLESHOOTING.md, PHILOSOPHY.md, MANIFESTO.md, and `locales/README.{th,ja,zh}.md`.
