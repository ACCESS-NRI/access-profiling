# Contributing

Contributions are welcome, as issues and pull requests on
[GitHub](https://github.com/ACCESS-NRI/access-profiling). For a significant change, we recommend discussing it in
an issue before opening a pull request.

## Development installation

Work in a virtual environment, with the package installed in editable mode together with its development and test
dependencies:

```bash
git clone https://github.com/ACCESS-NRI/access-profiling.git
cd access-profiling
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[devel,test]"
pre-commit install
```

## Tests and code style

Run the tests with:

```bash
pytest
```

The tests cover every line of the package, and pull requests are expected to keep it that way. pytest reports the
coverage in the terminal and writes it to `htmlcov/`.

The code is checked and formatted with [ruff](https://docs.astral.sh/ruff/), with lines of up to 120 characters,
through the pre-commit hooks. `pre-commit install` runs them on every commit, and you can run them on everything
with:

```bash
pre-commit run --all-files
```

## Docstrings

The [API reference](../api/index.md) is generated from the docstrings by
[mkdocstrings](https://mkdocstrings.github.io/), and the documentation is built in strict mode, so a docstring the
tools cannot read fails the build. The docstrings follow the Google style:

- Each section, such as `Args:`, `Returns:` or `Raises:`, is preceded by a blank line, or it is shown as plain text.
- An argument is written `name (type): description`, with the names the function actually takes, and a return
  value `type: description`.
- A description that runs onto several lines is indented one level more than its first line.
- The markup is Markdown, not reStructuredText: backticks for code, and fenced blocks for examples and for samples
  of the logs a parser reads.
- A public constant, such as a preset, has a docstring on the line after it. Without one it does not appear in the
  reference.

## Documentation

The documentation is built with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) from the
ACCESS-NRI documentation template, [documentation-infra](https://github.com/ACCESS-NRI/documentation-infra), and is
published on Read the Docs, which `.readthedocs.yaml` configures. Everything else is in `documentation/`:

- `mkdocs.yml`, the configuration, including the navigation;
- `docs/pages/`, the pages, one Markdown file each;
- `docs/assets/figures/`, the figures, drawn by `scripts/generate_figures.py`;
- `requirements.txt`, the packages the build needs.

To preview the site while you edit it, install the requirements in a separate environment and serve it:

```bash
pip install -r documentation/requirements.txt
mkdocs serve -f documentation/mkdocs.yml
```

The continuous integration builds the site with warnings treated as errors. To check that before pushing, run the
same build:

```bash
READTHEDOCS_CANONICAL_URL=https://access-profiling.access-hive.org.au/latest/ mkdocs build --strict -f documentation/mkdocs.yml
```

The variable stands in for the one Read the Docs sets, without which the build refuses to run. Note that the dates
at the foot of each page come from its git history, so a page that has not been committed yet makes the strict
build warn.

The figures of [Scaling studies](../user-guide/scaling-studies.md) and
[Best layout for a budget](../user-guide/best-layout.md) are drawn from synthetic data by the package's own plotting
functions. Draw them again after changing those functions:

```bash
python documentation/scripts/generate_figures.py
```

Read the Docs builds a preview of the site for every pull request, and a link to it is added to the pull request's
description. The `Check links` workflow checks the links of the preview.

## Releases

A release is made by tagging a commit of the main branch with a tag starting with `v`, such as `v0.3`, and pushing
the tag. The `CD` workflow then publishes the package on [PyPI](https://pypi.org/project/access-profiling/) and on the
[accessnri conda channel](https://anaconda.org/accessnri/access-profiling), and Read the Docs builds the
documentation of the new version.
