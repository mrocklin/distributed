Distributed follows a similar procedure for releasing as the core Dask project.

See https://github.com/dask/dask/blob/main/docs/release-procedure.md for instructions.

Pushing a release tag to `dask/distributed` triggers
`.github/workflows/release-publish.yml`, which builds and smoke-tests the wheel
and source distribution, verifies that they depend on the matching Dask release,
publishes them to PyPI with Trusted Publishing, and publishes the GitHub Release.

The PyPI Trusted Publisher should be configured with owner `dask`, repository
`distributed`, workflow filename `release-publish.yml`, and environment `pypi`.

The workflow can be rehearsed from GitHub Actions without publishing to PyPI by
running `Release Publisher` manually. Provide the Distributed version to
rehearse and a Dask version that is already available on PyPI. Manual runs
build distributions, check versions, run `twine check`, upload and download
workflow artifacts, wait for the requested Dask version on PyPI, run wheel and
source-distribution smoke tests, and run a dry-run publish job. They do not
enter the protected PyPI environment, upload to PyPI, or publish GitHub
Releases.

For coordinated Dask and Distributed releases, the Dask and Distributed tags may
be pushed together. The Distributed workflow waits until the matching
`dask==YYYY.M.X` wheel and source distribution are available on PyPI before
smoke-testing and publishing. The Distributed smoke tests install dependencies
from PyPI and assert that the installed Dask version matches the release, so the
matching Dask release must be resolvable first.

During this brief interval, `dask[distributed]` for the new version may not
resolve from PyPI until the matching Distributed package has been published. The
Distributed workflow keeps this window short by waiting on PyPI before
publishing. If the PyPI wait times out or the Distributed publish fails, rerun
it after fixing the issue and before announcing the release or proceeding to
conda-forge.

If PyPI publishing succeeds but the GitHub Release step fails, use GitHub
Actions' "Re-run failed jobs" option for that workflow run. Do not rerun the
full workflow from the beginning; PyPI rejects duplicate release files.
