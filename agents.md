# Agent guidelines for relaycache

## After every code change

Run pre-commit before declaring a task done — it covers formatting, linting,
type checking, security audits, and all tests:

```bash
pre-commit run --all-files
```

## Safety

The crate uses `#![forbid(unsafe_code)]`.  Do not add `unsafe` blocks.
If a dependency requires it, raise it explicitly with the user.
