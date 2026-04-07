# Documentation Ownership Map

This page defines a single source of truth for project documentation.

## Ownership Matrix

| Topic | Canonical File | Referenced From |
|---|---|---|
| Project overview and architecture | `README.md` | All module README files |
| Environment prerequisites (Python, Docker, Spark, GCP auth) | `docs/prerequisites.md` | Root + all runnable modules |
| End-to-end local execution | `docs/runbook-local.md` | `README.md` |
| End-to-end cloud execution | `docs/runbook-cloud.md` | `README.md`, cloud modules |
| Common troubleshooting and operations checks | `docs/operations.md` | Root + runtime modules |
| Data schema and dataset notes | `data/README.md` | Root, ML, streaming |
| Simulator (entrypoint + submodules) | `simulator/README.md` | Root |
| CSV simulator usage | `simulator/csv/README.md` | `simulator/README.md`, Root |
| Kafka local stack and topic tools | `simulator/kafka/README.md` | `simulator/README.md`, Root |
| Streaming scoring module behavior | `streaming/README.md` | Root |
| Alert consumer behavior | `consumers/README.md` | Root, Streaming |
| Terraform foundation provisioning | `infra/terraform/README.md` | Root |
| Batch-to-warehouse modeling | `dbt/README.md` | Root, Airflow |
| Airflow orchestration runbook | `airflow/README.md` | Root |
| BI dashboard setup | `dashboards/README.md` | Root |

## Consolidation Rules

1. If content is used by two or more modules, move it to `docs/` and link to it.
2. Module README files contain only module-specific commands and flags.
3. Root `README.md` stays as index + quickstart + links, not a full command encyclopedia.
4. Avoid copy/paste commands between module README files.
5. Keep heading structure consistent: `Purpose`, `Inputs/Outputs`, `Prerequisites`, `Run`, `Troubleshooting`.

## Backlog Maintenance

When adding a new module:

1. Decide canonical location for new instructions.
2. Add or update one row in this matrix.
3. Link from root and relevant module entrypoints.
