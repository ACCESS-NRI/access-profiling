# Copyright 2025 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

"""The ACCESS models and their configurations.

One module per model, or per family of models run the same way, as the Rose suites of `um_suites` are, each holding
their configurations and the presets this package ships, and one, `cice`, for the partitioning of CICE that
ACCESS-ESM1.6 and ACCESS-OM3 share. None of them holds a manager: a configuration says what the model is and how a
layout is realised in its own configuration files, and which workflow engine runs it is the manager's axis rather
than the model's.
"""
