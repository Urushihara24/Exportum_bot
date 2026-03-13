# Changelog

## 2026-03-13

### Status and flow consistency
- Synchronized offer/request/delivery transitions for exporter, farmer, and logistics request sources.
- Added stricter guards for assignee selection to prevent reassignment after deal/delivery has moved to active or terminal phases.
- Unified expeditor offer handling to use mutable/open expeditor status groups across list/view/choose flows.
- Prevented status rollback for selected expeditor offers (`in_progress` is no longer downgraded to `accepted`).
- Switched several delivery conflict checks to effective status evaluation to avoid false negatives on legacy records.

### UI/handler consistency
- Verified callback-button coverage for static and dynamic `callback_data` handlers.
- Checked naming consistency for function definitions (snake_case).

### Notes
- Local static check: `pyflakes` passes for `main.py`.
- Existing local tests in `tests/test_transition_maps.py` contain assertion expectations tied to older status strings and may require alignment with current logic.
