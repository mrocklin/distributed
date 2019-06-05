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
def serialize_cudf_dataframe(x):
    sub_headers = []
    arrays = []
    null_masks = []
    null_headers = []
    null_counts = {}

    for label, col in x.iteritems():
        try:
            # Non-string data
            header, [frame] = serialize_numba_ndarray(col.data.mem)
            header["name"] = label
            sub_headers.append(header)
            arrays.append(frame)

        except:
            # string data
            import numba.cuda as cuda
            import numpy as np
            s = col.data
            values = np.empty(s.size(), dtype=np.int8)
            offsets = np.empty(s.size()+1, dtype=np.int32)
            s.to_offsets(values,offsets)
            for item in [values,offsets]:
                item_ndarray = cuda.to_device(item)
                header, [frame] = serialize_numba_ndarray(item_ndarray)
                header["name"] = label
                sub_headers.append(header)
                arrays.append(frame)

        if col.null_count:
            header, [frame] = serialize_numba_ndarray(col.nullmask.mem)
            header["name"] = label
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
    }

    return header, arrays


@cuda_deserialize.register(cudf.DataFrame)
def deserialize_cudf_dataframe(header, frames):
    columns = header["columns"]
    n_columns = len(header["columns"])
    n_masks = len(header["null_subheaders"])

    masks = {}
    pairs = []

    #import pdb; pdb.set_trace()
    #print(n_columns)
    #print(len(frames))
    #print(header["subheaders"])

    for i in range(n_masks):
        subheader = header["null_subheaders"][i]
        frame = frames[n_columns + i]
        mask = deserialize_numba_ndarray(subheader, [frame])
        masks[subheader["name"]] = mask

    for subheader, frame in zip(header["subheaders"], frames[:n_columns]):
        name = subheader["name"]
        array = deserialize_numba_ndarray(subheader, [frame])

        if name in masks:
            series = cudf.Series.from_masked_array(array, masks[name])
        else:
            series = cudf.Series(array)
        pairs.append((name, series))

    return cudf.DataFrame(pairs)


@cuda_serialize.register(cudf.Series)
def serialize_cudf_series(x):
    sub_headers = []
    arrays = []
    null_masks = []
    null_headers = []
    null_counts = {}

    # Serialize data
    label = "data"
    header, [frame] = serialize_numba_ndarray(x.data.mem)
    header["name"] = label
    sub_headers.append(header)
    arrays.append(frame)

    # Serialize index (not sure how to do this yet..)
    #header, [frame] = serialize_numba_ndarray(x.index)
    #header["name"] = label
    #sub_headers.append(header)
    #arrays.append(frame)

    # Serialize null mask
    if x.null_count:
        header, [frame] = serialize_numba_ndarray(x.nullmask.mem)
        header["name"] = label
        null_headers.append(header)
        null_masks.append(frame)
        null_counts[label] = x.null_count

    arrays.extend(null_masks)

    header = {
        "subheaders": sub_headers,
        # TODO: the header must be msgpack (de)serializable.
        # See if we can avoid names, and just use integer positions.
        "null_counts": null_counts,
        "null_subheaders": null_headers,
    }

    return header, arrays


@cuda_deserialize.register(cudf.Series)
def deserialize_cudf_series(header, frames):
    n_masks = len(header["null_subheaders"])

    #import pdb; pdb.set_trace()
    masks = {}
    pairs = []
    n_columns = 1

    for i in range(n_masks):
        subheader = header["null_subheaders"][i]
        frame = frames[n_columns + i]
        mask = deserialize_numba_ndarray(subheader, [frame])
        masks[subheader["name"]] = mask

    subheader = header["subheaders"][0]
    frame = frames[0]
    name = subheader["name"]
    array = deserialize_numba_ndarray(subheader, [frame])

    if name in masks:
        series = cudf.Series.from_masked_array(array, masks[name])
    else:
        series = cudf.Series(array)

    return series