# Publishing Duraflow to PyPI

Duraflow is distributed under Apache-2.0. Version 1.0.0 uses execution protocol 2;
see the [upgrade guide](../guide/6_operations.md#upgrading-from-protocol-1) when
moving from the earlier alpha's protocol-1 histories.

## One-time account authorization

GitHub access alone does not grant PyPI upload rights. The PyPI account owner
must register this GitHub Actions identity as a Trusted Publisher. Do not put
PyPI passwords or API tokens in source, chat, workflow input fields, or logs.

For a new PyPI project, sign in at https://pypi.org/manage/account/publishing/
and add a **pending publisher**, using exactly these values:

| Field | Value |
| --- | --- |
| PyPI project name | `duraflow` |
| Owner | `hynlab` |
| Repository name | `duraflow` |
| Workflow name | `publish.yml` |
| Environment name | `pypi` |

The workflow name is the filename, not `Publish to PyPI` or the full path.
If you already own the PyPI project, add the same identity under that project's
Publishing settings instead. Pending publishers do not reserve project names.
The registration and first successful upload must both succeed.

Official documentation:
https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/
https://docs.pypi.org/trusted-publishers/using-a-publisher/

Configure protection/reviewer rules for the GitHub `pypi` environment as
appropriate. This workflow does not bypass any existing environment approval
requirements or PyPI authorization checks.

## Explicit release triggers

`.github/workflows/publish.yml` runs only on an explicit release request:

- Actions -> **Publish to PyPI** -> **Run workflow**, on `main`, with the exact
  version from `pyproject.toml`.
- A published GitHub release whose tag is `v<VERSION>` or `<VERSION>` and whose
  checked-out package version matches that tag.
- A commit to `main` changing `.github/pypi-release.txt`. This file is an explicit
  publishing request, not a status marker. Its contents must match the package
  version. The existing marker records the earlier `0.1.0a1` request; leave it
  unchanged when publishing through a release or manual workflow dispatch to
  avoid triggering a second upload.

Ordinary source/documentation commits do not publish. Forks cannot publish
through this workflow. API tokens are not used as an implicit fallback.

The workflow verifies the release request, runs lint, mypy, the unit/recovery
suite and native PostgreSQL/Pulsar tests, builds wheel and sdist, runs strict
Twine metadata checks, checks the license and typed marker, and smoke-tests the
installed wheel in an isolated environment outside the source checkout.
The exact two distributions are retained as the `pypi-distributions` artifact.

A separate job with only OIDC write permission publishes those artifacts using
PyPA's pinned publishing action. It does not check out or execute the project.
A final unprivileged job compares each published file's SHA256 against the
built artifact and installs the exact version from PyPI.

## Retrying authorization failures

An `invalid-publisher` error means PyPI did not match the workflow identity to
an authorized publisher. Register or correct the identity above, then open the
failed workflow run and choose **Re-run failed jobs**. This preserves the exact
built artifacts. Do not repeatedly retry without fixing the PyPI configuration.
Do not call an upload successful merely because the build passed.

Duplicate version/file errors deliberately fail; `skip-existing` is disabled.
If an upload succeeded only partially, inspect the PyPI files and checksums
before deciding how to recover. Do not delete a published version expecting to
reuse its filenames. Do not re-run a successfully published version blindly.

## Installing after successful publication

```bash
pip install duraflow
# Or pin the release:
pip install 'duraflow==1.0.0'
```

Python 3.12 or newer is required. These commands are valid only after the
publication and verification jobs succeed; the presence of this document does
not mean the PyPI project or version already exists.

The base package includes SQLAlchemy, psycopg, and pulsar-client. Release smoke
tests import these adapters from the installed wheel and from the published
package without extras. CI checks base-wheel installation on Linux and macOS
with Python 3.12 and 3.13.
