import cudf
from .cuda import cuda_serialize, cuda_deserialize
from .numba import serialize_numba_ndarray, deserialize_numba_ndarray


# TODO:
# 1. Just use positions
#    a. Fixes duplicate columns
#    b. Fixes non-msgpack-serializable names
# 2. cudf.Series
# 3. Serialize the index


@cuda_serialize.register(cudf.DataFrame)
@cuda_serialize.register(cudf.Series)
def serialize_cudf_dataframe(x):
    sub_headers = []
    arrays = []
    null_masks = []
    null_headers = []
    null_counts = {}
    n_string_columns = 0

    is_series = isinstance(x, cudf.Series)
    if is_series:
        x = x.to_frame()

    # Reset index so it can be serialized
    # TODO: make sure we are not overwriting a column named "index"
    serialize_index = False
    if x.index._start > 0:
        serialize_index = True
        x = x.reset_index()

    for label, col in x.iteritems():
        try:
            # Non-string data
            header, [frame] = serialize_numba_ndarray(col.data.mem)
            header["name"] = label
            header["string"] = None
            sub_headers.append(header)
            arrays.append(frame)

        except:
            # string data
            import numba.cuda as cuda
            import numpy as np
            from librmm_cffi import librmm
            s = col.data
            arr = np.arange(s.size(),dtype=np.int32)
            d_arr = librmm.to_device(arr)
            s.len(d_arr.device_ctypes_pointer.value)
            nchars = sum(d_arr.copy_to_host())
            values = np.empty(nchars, dtype=np.int8)
            offsets = np.empty(nchars+1, dtype=np.int32)
            s.to_offsets(values, offsets)
            n_string_columns += 1
            for i, item in enumerate([values, offsets]):
                item_ndarray = cuda.to_device(item)
                header, [frame] = serialize_numba_ndarray(item_ndarray)
                header["name"] = label
                header["string"] = i+1
                header["nstrings"] = len(s)
                sub_headers.append(header)
                arrays.append(frame)

        if col.null_count:
            header, [frame] = serialize_numba_ndarray(col.nullmask.mem)
            header["name"] = label
            header["string"] = None
            null_headers.append(header)
            null_masks.append(frame)
            null_counts[label] = col.null_count

    arrays.extend(null_masks)

    header = {
        "subheaders": sub_headers,
        # TODO: the header must be msgpack (de)serializable.
        # See if we can avoid names, and just use integer positions.
        "columns": x.columns.tolist(),
        "null_counts": null_counts,
        "null_subheaders": null_headers,
        "n_string_columns": n_string_columns,
        "index": serialize_index,
        "series": is_series,
    }

    return header, arrays


@cuda_deserialize.register(cudf.DataFrame)
@cuda_deserialize.register(cudf.Series)
def deserialize_cudf_dataframe(header, frames):
    n_columns = len(header["columns"])
    n_masks = len(header["null_subheaders"])
    n_string_columns = header["n_string_columns"]
    is_series = header["series"]

    masks = {}
    pairs = []

    for i in range(n_masks):
        subheader = header["null_subheaders"][i]
        frame = frames[n_columns + n_string_columns + i]
        mask = deserialize_numba_ndarray(subheader, [frame])
        masks[subheader["name"]] = mask

    values = {}
    series_name = None
    for i in range(len(header["subheaders"])):
        subheader = header["subheaders"][i]
        frame = frames[i]
        name = subheader["name"]
        string_col = subheader["string"]
        array = deserialize_numba_ndarray(subheader, [frame])

        if string_col:
            if string_col == 1:
                values = array[:]
                continue
            else:
                import nvstrings
                vals = values.copy_to_host()
                offsets = array.copy_to_host()
                array = nvstrings.from_offsets(vals, offsets, subheader["nstrings"])
                values = None
        
        if name in masks:
            series = cudf.Series.from_masked_array(array, masks[name])
        else:
            series = cudf.Series(array)

        pairs.append((name, series))
        if is_series and name != "index":
            series_name = name

    df = cudf.DataFrame(pairs)

    if header["index"]:
        df = df.set_index(df["index"])
        df.index.name = None
        df = df.drop(["index"], axis=1)

    if is_series:
        df = df[series_name]
        df.name = None

    return df
