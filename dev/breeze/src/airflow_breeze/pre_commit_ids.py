# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE
# OVERWRITTEN WHEN RUNNING PRE_COMMIT CHECKS.
#
# IF YOU WANT TO MODIFY IT, YOU SHOULD MODIFY THE TEMPLATE
# `pre_commit_ids_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze` DIRECTORY

PRE_COMMIT_LIST = [
    "all",
    "black",
    "blacken-docs",
    "check-airflow-config-yaml-consistent",
    "check-airflow-provider-compatibility",
    "check-apache-license-rat",
    "check-base-operator-partial-arguments",
    "check-base-operator-usage",
    "check-boring-cyborg-configuration",
    "check-breeze-top-dependencies-limited",
    "check-builtin-literals",
    "check-changelog-has-no-duplicates",
    "check-core-deprecation-classes",
    "check-daysago-import-from-utils",
    "check-decorated-operator-implements-custom-name",
    "check-docstring-param-types",
    "check-example-dags-urls",
    "check-executables-have-shebangs",
    "check-extra-packages-references",
    "check-extras-order",
    "check-for-inclusive-language",
    "check-hooks-apply",
    "check-incorrect-use-of-LoggingMixin",
    "check-init-decorator-arguments",
    "check-lazy-logging",
    "check-links-to-example-dags-do-not-use-hardcoded-versions",
    "check-merge-conflict",
    "check-newsfragments-are-valid",
    "check-no-providers-in-core-examples",
    "check-no-relative-imports",
    "check-only-new-session-with-provide-session",
    "check-persist-credentials-disabled-in-github-workflows",
    "check-pre-commit-information-consistent",
    "check-provide-create-sessions-imports",
    "check-provider-yaml-valid",
    "check-providers-init-file-missing",
    "check-providers-subpackages-init-file-exist",
    "check-pydevd-left-in-code",
    "check-revision-heads-map",
    "check-safe-filter-usage-in-html",
    "check-setup-order",
    "check-start-date-not-used-in-defaults",
    "check-system-tests-present",
    "check-system-tests-tocs",
    "check-xml",
    "codespell",
    "compile-www-assets",
    "compile-www-assets-dev",
    "create-missing-init-py-files-tests",
    "debug-statements",
    "detect-private-key",
    "doctoc",
    "end-of-file-fixer",
    "fix-encoding-pragma",
    "flynt",
    "identity",
    "insert-license",
    "isort",
    "lint-chart-schema",
    "lint-css",
    "lint-dockerfile",
    "lint-helm-chart",
    "lint-json-schema",
    "lint-markdown",
    "lint-openapi",
    "mixed-line-ending",
    "pretty-format-json",
    "pydocstyle",
    "python-no-log-warn",
    "pyupgrade",
    "replace-bad-characters",
    "rst-backticks",
    "run-flake8",
    "run-mypy",
    "run-shellcheck",
    "static-check-autoflake",
    "trailing-whitespace",
    "ts-compile-and-lint-javascript",
    "update-black-version",
    "update-breeze-cmd-output",
    "update-breeze-readme-config-hash",
    "update-common-sql-api-stubs",
    "update-er-diagram",
    "update-extras",
    "update-in-the-wild-to-be-sorted",
    "update-inlined-dockerfile-scripts",
    "update-local-yml-file",
    "update-migration-references",
    "update-providers-dependencies",
    "update-spelling-wordlist-to-be-sorted",
    "update-supported-versions",
    "update-vendored-in-k8s-json-schema",
    "update-version",
    "yamllint",
    "yesqa",
]
