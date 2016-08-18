#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc
from bokeh.layouts import column, row
from toolz import valmap

from distributed.bokeh.worker_monitor import worker_table_plot, worker_table_update
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'scale_width'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

source, table, mem_plot = worker_table_plot(width=WIDTH)
def worker_update():
    with log_errors():
        try:
            msg = messages['workers']['deque'][-1]
        except IndexError:
            return
        worker_table_update(source, msg)
doc.add_periodic_callback(worker_update, messages['workers']['interval'])


layout = column(
    table,
    mem_plot,
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
