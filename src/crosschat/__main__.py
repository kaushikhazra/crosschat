"""Module entrypoint: ``python -m crosschat <command> ...``.

Equivalent to the ``crosschat`` console script, and the form to prefer when a
caller resolves the interpreter at runtime.

The console script is a generated shim with the interpreter's absolute path
baked in at install time. That is fine until the environment moves — rename or
relocate the directory holding the venv and every shim inside it points at a
python that is no longer there. Invoking the module through an interpreter you
located *now* has no such stored path: find the python, run the module, and
relocation is a non-event.

This matters here because a project's crosschat may live in its own
``.claude/.venv`` rather than on PATH, so callers have to resolve an
interpreter anyway.
"""

from crosschat.cli import main

if __name__ == "__main__":
    main()
