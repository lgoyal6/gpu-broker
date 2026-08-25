# Contributing

This is a club tool. Other members have to be able to read it and change it, so
the bar for a change is "somebody who has not seen this file can follow it", not
cleverness.

## Getting set up

```bash
uv venv --python 3.11 && uv pip install -e '.[dev]'
pytest -m "not aws"      # the fast loop, ~80s
```

You need no AWS account, no GPU, and no network. If a change makes any of those
necessary to run the tests, that is the thing to fix.

## Before you open a PR

```bash
pytest                                        # everything, ~4min
ruff check --select F,E9 gpu_broker tests
gpu doctor                                    # still green on a fresh state dir
```

CI runs the same three on Python 3.11 and 3.12, with deliberately junk AWS
credentials, plus a CLI smoke sequence on a clean install.

## How this codebase is organised

- **`store.py` is the only module that writes SQL.** Everything else asks it.
- **Backends know nothing about storage, budgets, or the queue.** They launch
  things, report what they see, and stop things. All policy lives above them.
- **`poll()` is read-only.** A mutation hidden inside a read cannot be dry-run
  and will eventually happen twice.
- **One state transition is one committed transaction**, together with its ledger
  consequence.
- **The ledger is append-only.** A test greps for `UPDATE ledger` and fails if
  anything mutates it.

## Writing tests

Tests here are the documentation that cannot go stale, so:

**Name the behaviour, not the function.**
`test_a_light_user_goes_ahead_of_a_heavy_one_who_submitted_first`, not
`test_order_queue`.

**Say why in the docstring when the why is not obvious.** The best tests in this
repo explain the failure they exist to prevent - a reader should learn something
about the system from the test even if it never fails.

**Assert on the property, not the implementation.** The Phase 4 gate does not
check that jobs reached `COMPLETED`; it checks they finished with the *right
answer*, which caught a resume off-by-one that every state assertion missed.

**Prefer a real double over a hand-written one.** `moto` answers the actual AWS
APIs; `asyncssh` runs a real SSH server. A fake written from the same assumptions
as the code tests the assumptions, not the code.

## Things that are deliberately absent

Before adding one of these, read the reasoning in [NOTES.md](NOTES.md) - they are
choices, not oversights.

- `gpu reap --force`. A test asserts there is no such flag.
- Automatic reclaim by default.
- A build server for environments.
- Pruning of checkpoints, images, or virtualenvs.
- A scheduler for the digest.

Ideas that came up and were not built go in `NOTES.md` with the reason, not in
the code as a `TODO`.

## Style

- Boring, readable Python. Match the surrounding code.
- Comments explain *why*, and the interesting ones explain what goes wrong
  otherwise. Do not comment what the code already says.
- Error messages say what to do next, not just what went wrong. If you add one
  that a member will see, read it back and ask whether a sophomore could act on
  it.
- Money is `Decimal`, never `float`. Timestamps are ISO-8601 UTC.
