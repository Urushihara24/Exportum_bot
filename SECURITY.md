# Security Policy

## Scope

Security reports are welcome for issues involving Telegram bot credentials, administrator access, persisted operational data, role/permission checks, notifications, exports, or deployment configuration.

Examples include:

- exposure of `BOT_TOKEN`, administrator identifiers, Google/API credentials, or persisted user data;
- bypass of role or administrator authorization;
- unintended bulk notifications or state-changing operations;
- unsafe persistence/export behavior involving user, company, shipment, or deal data;
- dependency or deployment issues that could expose runtime secrets or operational state.

## Reporting

Do **not** publish tokens, credentials, personal/company data, production identifiers, exported operational data, or exploit details in a public issue.

Use GitHub private vulnerability reporting when it is available for this repository. If a private GitHub report is not available, send a short sanitized summary through the work contact listed on the maintainer profile and keep secrets/redacted records out of the initial message.

Include, when possible:

- affected commit/version and role/workflow;
- minimal reproduction steps using placeholder data;
- expected vs actual authorization/state behavior;
- possible operational or data impact;
- sanitized logs/screenshots.

## Secret and data handling

Runtime `.env` files and credentials must remain outside Git. Reports and test artifacts must not introduce real Telegram tokens, Google credentials, user/company identifiers, or production operational data into the repository.

Security-related dependency changes should preserve the verified Python 3.11 runtime baseline and must be regression-checked against the existing workflow/state behavior.