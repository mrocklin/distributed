from distributed.versions import get_versions, error_message


def test_error_message():
    a = get_versions()
    b = get_versions()
    b["packages"]["required"]["dask"] = "0.0.0"

    msg = error_message(a, {"w-1": b}, a)
    assert msg
    assert "dask" in msg
    assert "0.0.0" in msg
