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

The tests cover every line of the package, and we ask that pull requests keep it that way. pytest shows the
coverage in the terminal, and also writes it to `htmlcov/`.

The code is checked and formatted with [ruff](https://docs.astral.sh/ruff/), with lines of up to 120 characters,
through the pre-commit hooks. `pre-commit install` runs them on every commit, and you can run them on everything
with:

```bash
pre-commit run --all-files
```

## Docstrings

The [API reference](../api/index.md) is generated from the docstrings by
[mkdocstrings](https://mkdocstrings.github.io/). The documentation is built in strict mode, which means that the
build fails if a docstring cannot be read. The docstrings follow the Google style, with a few things to keep in
mind:

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
READTHEDOCS_CANONICAL_URL=https://access-profiling.readthedocs.io/latest/ mkdocs build --strict -f documentation/mkdocs.yml
```

The variable is normally set by Read the Docs, and the build does not run without it. Note that the dates at the
foot of each page come from the git history. A page that has not been committed therefore causes a warning in the
strict build.

The figures of [Scaling studies](../user-guide/scaling-studies.md) and
[Best layout for a budget](../user-guide/best-layout.md) are made from synthetic data, with the plotting functions
of the package. If you change those functions, make the figures again:

```bash
python documentation/scripts/generate_figures.py
```

Read the Docs builds a preview of the site for every pull request, and a link to it is added to the pull request's
description. The `Check links` workflow checks the links of the preview.

## Releases

To make a release, tag a commit of the main branch with a tag that starts with `v`, such as `v0.3`, and push the
tag. The `CD` workflow then publishes the package on [PyPI](https://pypi.org/project/access-profiling/) and on the
[accessnri conda channel](https://anaconda.org/accessnri/access-profiling), and Read the Docs builds the
documentation of the new version.
