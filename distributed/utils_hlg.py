from __future__ import annotations

from numbers import Number

from tlz import keymap, valmap

import dask
from dask.core import get_deps
from dask.highlevelgraph import HighLevelGraph
from dask.utils import stringify


def _materialize_and_process_hlg(
    hlg: HighLevelGraph,
    keys: list[str],
    priority=None,
    resources=None,
    retries=None,
    user_priority=0,
    workers=None,
    allow_other_workers=None,
    annotations=None,
) -> dict:
    """Materialize and process a HighLevelGraph object

    The output of this function is a dictionary of (most)
    key-word arguments to ``distributed.scheduler.update_graph``.
    """

    from distributed.utils_comm import unpack_remotedata
    from distributed.worker import dumps_task

    if annotations is None:
        annotations = {}

    workers = workers or annotations.pop("workers", None)
    allow_other_workers = allow_other_workers or annotations.pop(
        "allow_other_workers", None
    )
    if retries is None:
        retries = annotations.pop("retries", None)
    resources = resources or annotations.pop("resources", None)
    user_priority = annotations.pop("priority", user_priority)

    if isinstance(workers, (str, Number)):
        workers = [workers]
    if isinstance(workers, (tuple, set)):
        workers = list(workers)
    if isinstance(workers, list):
        restrictions = workers
    elif workers is None:
        restrictions = []
    else:
        raise TypeError("Workers must be a list or set of workers or None")

    # Materialized the HLG
    dsk = dict(hlg)

    if allow_other_workers:
        loose_restrictions = set(dsk)
    else:
        loose_restrictions = set()

    dependencies, dependents = get_deps(dsk)

    # Remove `Future` objects from graph and note any future dependencies
    dsk2 = {}
    fut_deps = {}
    for k, v in dsk.items():
        dsk2[k], futs = unpack_remotedata(v, byte_keys=True)
        if futs:
            fut_deps[k] = futs
    dsk = dsk2

    # - Add in deps for any tasks that depend on futures
    for k, futures in fut_deps.items():
        dependencies[k].update(f.key for f in futures)

    pre_stringify = set(dsk)
    dsk = {stringify(k): stringify(v, exclusive=hlg) for k, v in dsk.items()}

    def process(x, keys=dsk, string_keys=pre_stringify):
        if callable(x):
            return {stringify(k): x(k) for k in keys}
        elif isinstance(x, (int, dict, tuple, set, list)):
            return {stringify(k): x for k in string_keys or map(stringify, keys)}
        elif isinstance(x, dict):
            return keymap(stringify, x)
        raise TypeError()

    if annotations:
        annotations = process(annotations)
    if retries:
        retries = process(retries)
    if resources:
        resources = process(resources)
    if user_priority:
        user_priority = process(user_priority)
    if restrictions:
        _restrictions = process(restrictions)
    else:
        _restrictions = {}

    for layer in hlg.layers.values():
        if not layer.annotations:
            continue
        layer_annotations: dict = dict(layer.annotations)
        if "retries" in layer_annotations:
            retries = retries or {}
            d = process(layer_annotations["retries"], keys=layer, string_keys=None)
            retries.update(d)  # TODO: there is an implicit ordering here
        if "priority" in layer_annotations:
            user_priority = user_priority or {}
            d = process(
                layer_annotations.pop("priority"),
                keys=layer,
                string_keys=None,
            )
            user_priority.update(d)  # TODO: there is an implicit ordering here
        if "resources" in layer_annotations:
            resources = resources or {}
            d = process(layer_annotations["resources"], keys=layer, string_keys=None)
            resources.update(d)  # TODO: there is an implicit ordering here
        if "workers" in layer_annotations:
            if isinstance(layer_annotations["workers"], (str, int)):
                layer_annotations["workers"] = (layer_annotations["workers"],)
            _restrictions = _restrictions or {}
            d = process(layer_annotations["workers"], keys=layer, string_keys=None)
            _restrictions.update(d)  # TODO: there is an implicit ordering here

        if "allow_other_workers" in layer_annotations:
            if layer_annotations["allow_other_workers"] is True:
                loose_restrictions.update(set(map(stringify, layer)))

        if layer_annotations:
            d = process(layer_annotations, keys=layer, string_keys=None)
            annotations.update(d)

    dsk = valmap(dumps_task, dsk)

    dependencies = {
        stringify(k): {stringify(dep) for dep in deps}
        for k, deps in dependencies.items()
    }

    # Remove any self-dependencies (happens on test_publish_bag() and others)
    for k, v in dependencies.items():
        deps = set(v)
        if k in deps:
            deps.remove(k)
        dependencies[k] = deps

    if priority is None:
        # Removing all non-local keys before calling order()
        dsk_keys = set(dsk)  # intersection() of sets is much faster than dict_keys
        stripped_deps = {
            k: v.intersection(dsk_keys)
            for k, v in dependencies.items()
            if k in dsk_keys
        }
        priority = dask.order.order(dsk, dependencies=stripped_deps)

    # Return (most) graph-dependent scheduler.update_graph kwargs
    return dict(
        tasks=dsk,
        keys=keys,
        dependencies=dependencies,
        restrictions=_restrictions,
        priority=priority,
        loose_restrictions=loose_restrictions,
        resources=resources,
        retries=retries,
        user_priority=user_priority,
        annotations=annotations,
    )
