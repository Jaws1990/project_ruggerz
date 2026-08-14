# project_ruggerz

## Lakehouse Schema Management

Fabric Git and Deployment Pipelines do not currently deploy Lakehouse schemas or table schemas. Schema management is therefore handled through the **Delta Table Definitions notebook**, which is source controlled and run in each environment.

### Approach

- Create schemas using `CREATE SCHEMA IF NOT EXISTS`.
- Create new tables using `CREATE TABLE IF NOT EXISTS`.
- Manage changes to existing tables through explicit schema migrations.
- Use `ADD COLUMNS IF NOT EXISTS` for non-destructive column additions.
- Handle renames and removals through explicit `ALTER TABLE` statements.
- Do not automatically drop columns or tables.
- Keep all schema changes in Git so Dev → Test → Prod remains reproducible.

The Delta Table Definitions notebook is therefore the **source-controlled mechanism for managing Lakehouse structure across environments**.
