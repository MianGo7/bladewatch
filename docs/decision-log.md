# Decision log

Architecture, tooling, and design decisions in the form of architecture
decision records (ADR). Each record states the context, the decision, the
alternatives that were rejected, and the consequences. A decision that changes
later is not edited in place; a new record supersedes it and says so.

Template for every record:

    ## ADR-NNNN: Title

    Date: YYYY-MM-DD. Status: accepted.

    **Context.** The situation and the forces that made a decision necessary.

    **Decision.** What was decided.

    **Alternatives.** What else was considered and why it was rejected.

    **Consequences.** What follows from the decision, including its costs.

---

## ADR-0001: Python 3.13 with uv and a src layout

Date: 2026-10-18. Status: accepted.

**Context.** The prototype consists of a Python package with a simulator, a
stream processor, and a measurement module, and its results have to be
reproducible on another machine from the repository alone. The interpreter
that macOS provides is version 3.9, which is too old for the language
features the package relies on, so the interpreter and the dependencies have
to be managed by the project.

**Decision.** The project requires Python 3.13, pinned for local development
in `.python-version`, and is managed with uv. The dependencies are declared in
`pyproject.toml` with exact versions and resolved in `uv.lock`, which is
committed. The package lives in `src/bladewatch/` and is built with the uv
build backend, so that the tests import the installed package and never the
working directory by accident.

**Alternatives.** Poetry offers the same lock file workflow but is a
separate tool to install and is slower to resolve, while uv covers the
interpreter, the environment, the lock file, and the build with one binary.
A plain virtual environment with `pip` and a `requirements.txt` was rejected
because it does not lock transitive dependencies, which would undermine
NFR5. A flat layout without `src/` was rejected because it lets a test pass
against uninstalled code.

**Consequences.** Every contributor needs uv, which downloads the interpreter
when it is missing. Any dependency change goes through `uv add` and
`uv remove` and needs its own record in this log. At this stage the project has
no runtime dependency at all.

## ADR-0002: pytest, Ruff, and a continuous integration workflow

Date: 2026-10-18. Status: accepted.

**Context.** The quality criterion rewards tests and a reproducible build, and
the conventions of the project (complete type hints, docstrings, no `print`
in the services, no global random state, no commented out code) are only
worth stating if a tool enforces them. The streaming part cannot be tested
without Docker, so unit and integration tests have to be separable.

**Decision.** Tests are written with pytest 9.1.1. The marker `integration`
is registered and `--strict-markers` is set, so that a misspelled marker
fails instead of silently selecting nothing. Ruff 0.17.1 is the only tool for
formatting and linting. The selected rule families are the pycodestyle and
Pyflakes defaults, import order, bugbear, modern syntax, naming, docstrings in
the Google convention, type annotations, `print` calls, legacy NumPy random
state, import conventions, early returns, simplifications, and commented out
code. The only exemption is the docstring family for files under `tests/`,
because the plain English test names already state the behaviour and a
docstring would repeat them. A GitHub Actions workflow runs the format check,
the lint, and the unit tests on every push and pull request, with the
integration tests excluded because the runner has no Compose stack. The
workflow pins the runner image, both actions, and the uv version to exact
tags.

**Alternatives.** The standard library `unittest` was rejected because it has
no parametrisation or fixtures of comparable convenience. Black, isort,
flake8, and pylint were rejected because Ruff covers their functions in one
dependency with one configuration. Pinning the actions to commit hashes would
be stricter against a moved tag but makes the workflow harder to read, and
the repository is a prototype without secrets in the workflow. A static type
checker such as mypy was not added: the annotation rule only checks that hints
exist and not that they are correct, which is a limit of the current checks
and is revisited if type errors show up in the later modules.

**Consequences.** Two development dependencies are fixed by this record. The
workflow could only be checked for well-formed YAML locally, so its first real
run happens when the repository is pushed to GitHub.
