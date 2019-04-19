import pytest

from dask.distributed import Client
from distributed.versions import get_versions, error_message
from distributed.utils_test import gen_cluster


def test_error_message():
    a = get_versions()
    b = get_versions()
    b["packages"]["dask"] = "0.0.0"

    msg = error_message(a, {"w-1": b}, a)
    assert msg
    assert "dask" in msg
    assert "0.0.0" in msg


@gen_cluster()
def test_warning(s, a, b):
    s.workers[a.address].versions["packages"]["dask"] = "0.0.0"

    with pytest.warns(None) as record:
        client = yield Client(s.address, asynchronous=True)

    assert record
    assert any("dask" in str(r.message) for r in record)
    assert any("0.0.0" in str(r.message) for r in record)
    assert any(a.address in str(r.message) for r in record)
