# GS-MAD: GS Manufacturing AI-Driven Development

> Risk-Gated CRISP-ML(Q) with Kanban Execution
> Solo AI Engineer team | Manufacturing context | Thai + English

---

## Quick Start

1. Claude Code reads this file automatically on startup
2. Type `/gs-help` to see where you are and what to do next
3. Use slash commands below to invoke the right agent for your phase

---

## Slash Command Routing

When the user types a slash command, read the corresponding agent file from `.gs-mad/agents/` and adopt that role completely. Do not mix agent behaviors.

| Command | Agent File | Phase | Track | Purpose |
|---------|-----------|-------|-------|---------|
| `/gs-help` | `gs-help.md` | Any | Both | Where am I? What next? |
| `/qualify` | `gs-qualify.md` | P1 | Both | Business understanding + Gate 1 |
| `/research` | `gs-research.md` | P1-P3 | Both | Paper/model/benchmark search |
| `/ml-eng` | `gs-ml-engineer.md` | P2-P4, P8 | Both | Training, MLflow, DVC, drift |
| `/edge-eng` | `gs-edge-engineer.md` | P4-P7 | IoT | TensorRT, Jetson, hardware |
| `/build` | `gs-builder.md` | P4-P6 | App | Spec-first dev, test backpressure |
| `/fahfon` | `gs-fahfon.md` | P4-P6 | App | Frontend standard (Kaizen/FIXA) |
| `/review` | `gs-reviewer.md` | Gates | Both | Checklist pass/fail, gate enforcement |

### How Routing Works

1. User types `/command` (e.g., `/qualify`)
2. Claude Code reads `.gs-mad/agents/gs-qualify.md`
3. Adopt the role, workflow, and constraints defined in that file
4. Read `.gs-mad/config.yaml` for project context (track, phase, project name)
5. Execute the agent's workflow — do not freelance beyond the agent's scope
6. On completion, suggest the next logical command (handoff)

### Cross-Agent Rules

These rules apply regardless of which agent is active:

1. **Always read config.yaml first** — Know the track (iot/app), current phase, and project name before acting.
2. **Manufacturing-first** — Low error tolerance. Hardware dependencies. Cross-department politics. Never assume "move fast and break things."
3. **MLOps mandatory** — No experiment without MLflow tracking. No dataset without DVC versioning. No exceptions.
4. **Gate-based progression** — Phases advance only when `/review` passes the gate checklist. No skipping.
5. **Anti-overengineering** — Challenge unnecessary infra. If someone asks for Redis, K8s, or Kafka, ask "Why? What problem does this solve that a simpler tool can't?"
6. **Business language for stakeholders** — Present results in THB saved, hours reduced, defect rate improved. Not F1-score or mAP.
7. **Thai + English** — Accept Thai input. Write code in English. Stakeholder documents follow `config.yaml` language preference.
8. **Solo team** — No ceremony. Everything must be runnable by 1 person. If a process requires 3 people, simplify it.
9. **Spec-first for /build** — The `/build` agent requires a deployment-spec.md before writing any code.
10. **Research-first for /ml-eng** — The `/ml-eng` agent requires `/research` output before model selection.

---

## Project Configuration

The file `.gs-mad/config.yaml` stores project state. Every agent reads it. Only `/gs-help` and `/review` update it.

```yaml
# .gs-mad/config.yaml
project_name: ""
track: ""          # "iot" or "app"
current_phase: ""  # P1, P2, P3, ... P8
language: "en"     # "en" or "th" for stakeholder docs
```

---

## Track Selection

Every project starts at Phase 1. Track is determined during `/qualify`:

- **IoT Track** — Camera inspection, object counting, edge AI, sensor-based prediction
- **App Track** — LLM chatbot, OCR application, forecasting dashboard, agentic AI

---

## Phase Flow

### IoT Track
P1 Business Understanding → P2 Site Survey → P3 Model Training → P4 Edge Optimization → Gate 2 Demo → P5 HW Setup → P6 UAT → P7 Deploy → P8 Monitoring

### App Track
P1 Business Understanding → P2 POC Build → P3 User Validation → Model Strategy → P4 Development → P5 UAT → P6 Deploy → P7 Monitoring

---

## Feedback Loops

| Loop | Trigger | Goes Back To | Max |
|------|---------|-------------|-----|
| L1 | Edge accuracy drop > 5% | P3 retrain | 3 |
| L2 | Site data differs from lab | P2 re-survey | 2 |
| L3 | User score 5-7 | P2 tweak POC | 2 |
| L4 | Fundamental misunderstanding | P1 re-interview | 2 then kill |
| L5 | Bug in UAT | P4 fix | unlimited |
| L6 | Drift in production | P3 retrain | unlimited |

---

## Templates & Checklists

- Templates: `.gs-mad/templates/` — Deliverable formats for each phase
- Checklists: `.gs-mad/checklists/` — Gate pass/fail criteria

Agents reference these when producing deliverables or running gate reviews.

---

## Techstack

### Shared
AWS (VPC, EC2, S3, EFS, SageMaker) | MLflow | DVC | GitHub

### IoT Track Additions
YOLO, PatchCore, AnomalyCLIP | TensorRT, ONNX | Jetson, reCamera | NodeRED, MQTT | Grafana, InfluxDB

### App Track Additions
FastAPI or Express+TS | React/Streamlit | Gemini/Claude/OpenAI API | vLLM (self-host) | LangGraph | Langfuse | Supabase
