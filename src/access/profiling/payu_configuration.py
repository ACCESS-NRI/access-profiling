# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""What a Payu-driven application states about itself.

Everything here knows that Payu is the runner: that it archives a run under `config.yaml`, that it names the
model's standard output after the `model` key that file states. None of it belongs in the generic contract,
which must hold for a runner that does none of those things.

It is kept apart from `payu_manager` deliberately. That module reaches for the experiment generator and the
experiment runner, and so for Payu itself; an application only describes what is to be profiled and should
not have to import a job-submission stack to do it.
"""

from abc import abstractmethod
from pathlib import Path

from access.config import YAMLParser

from access.profiling.configuration import LogLocator, ModelConfiguration


def payu_model_stdout() -> LogLocator:
    """Returns a locator for the standard output Payu captured from the model.

    Payu names it after the model it ran, which the archived `config.yaml` records, so the name is read from
    there rather than assumed.

    Returns:
        LogLocator: The locator.
    """

    def locate(output_dir: Path) -> Path | None:
        config_path = output_dir / "config.yaml"
        if not config_path.is_file():
            return None
        payu_config = YAMLParser().parse(config_path.read_text())
        model = payu_config.get("model")
        return None if model is None else output_dir / f"{model}.out"

    return locate


class PayuConfiguration(ModelConfiguration):
    """Abstract base class for a configuration of a model driven by Payu."""

    @property
    @abstractmethod
    def model_type(self) -> str:
        """Returns the model type identifier, as Payu defines it.

        Returns:
            str: The identifier, e.g. "access-esm1.6".
        """
