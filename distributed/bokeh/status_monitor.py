from __future__ import print_function, division, absolute_import

import json

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback

from ..core import rpc
from ..utils import is_kernel, log_errors, key_split
from ..executor import default_executor
from ..scheduler import Scheduler
from ..diagnostics.progress_stream import (nbytes_bar, task_stream_palette,
        incrementing_index)

try:
    from bokeh.palettes import Spectral11, Spectral9, viridis
    from bokeh.models import ColumnDataSource, DataRange1d, HoverTool, Range1d
    from bokeh.plotting import figure
    from bokeh.io import curstate, push_notebook
except ImportError:
    Spectral11 = None


def task_stream_plot(sizing_mode='scale_width', **kwargs):
    data = {'start': [], 'duration': [],
            'key': [], 'name': [], 'color': [],
            'worker': [], 'y': [], 'worker_thread': [], 'alpha': []}

    source = ColumnDataSource(data)
    x_range = DataRange1d(range_padding=0)

    fig = figure(
        x_axis_type='datetime', title="Task stream",
        tools='xwheel_zoom,xpan,reset,box_zoom', toolbar_location='above',
        sizing_mode=sizing_mode, x_range=x_range, **kwargs
    )
    fig.rect(
        x='start', y='y', width='duration', height=0.8,
        fill_color='color', line_color='color', line_alpha=0.6, alpha='alpha',
        line_width=3, source=source
    )
    fig.xaxis.axis_label = 'Time'
    fig.yaxis.axis_label = 'Worker Core'
    fig.ygrid.grid_line_alpha = 0.4
    fig.xgrid.grid_line_color = None
    fig.min_border_right = 35
    fig.yaxis[0].ticker.num_minor_ticks = 0

    hover = HoverTool()
    fig.add_tools(hover)
    hover = fig.select(HoverTool)
    hover.tooltips = """
    <div>
        <span style="font-size: 14px; font-weight: bold;">Key:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">Duration:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@duration</span>
    </div>
    """
    hover.point_policy = 'follow_mouse'

    return source, fig



def task_stream_append(lists, msg, workers, palette=task_stream_palette):
    start, stop = msg['compute_start'], msg['compute_stop']
    lists['start'].append((start + stop) / 2 * 1000)
    lists['duration'].append(1000 * (stop - start))
    key = msg['key']
    name = key_split(key)
    if msg['status'] == 'OK':
        color = palette[incrementing_index(name) % len(palette)]
    else:
        color = 'black'
    lists['key'].append(key)
    lists['name'].append(name)
    lists['color'].append(color)
    lists['alpha'].append(1)
    lists['worker'].append(msg['worker'])

    worker_thread = '%s-%d' % (msg['worker'], msg['thread'])
    lists['worker_thread'].append(worker_thread)
    if worker_thread not in workers:
        workers[worker_thread] = len(workers)
    lists['y'].append(workers[worker_thread])

    if msg.get('transfer_start') is not None:
        start, stop = msg['transfer_start'], msg['transfer_stop']
        lists['start'].append((start + stop) / 2 * 1000)
        lists['duration'].append(1000 * (stop - start))

        lists['key'].append(key)
        lists['name'].append('transfer-to-' + name)
        lists['worker'].append(msg['worker'])
        lists['color'].append('red')
        lists['alpha'].append('0.8')
        lists['worker_thread'].append(worker_thread)
        lists['y'].append(workers[worker_thread])


def nbytes_plot(**kwargs):
    data = {'name': [], 'left': [], 'right': [], 'center': [], 'color': [],
            'percent': [], 'MB': [], 'text': []}
    source = ColumnDataSource(data)
    fig = figure(title='Memory Use', tools='', toolbar_location=None, **kwargs)
    fig.quad(source=source, top=1, bottom=0,
             left='left', right='right', color='color', alpha=0.8)
    fig.text(source=source, x='center', y=0.5, text='text',
             text_baseline='middle', text_align='center')

    fig.grid.grid_line_color = None
    fig.grid.grid_line_color = None
    fig.axis.visible = False
    fig.outline_line_color = None
    fig.yaxis.visible = False

    hover = HoverTool()
    fig.add_tools(hover)
    hover = fig.select(HoverTool)
    hover.tooltips = """
    <div>
        <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">Percent:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@percent</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">MB:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@MB</span>
    </div>
    """
    hover.point_policy = 'follow_mouse'

    return source, fig


def progress_plot(**kwargs):
    from ..diagnostics.progress_stream import progress_wedge
    data = progress_wedge({'all': {}, 'memory': {},
                           'erred': {}, 'released': {}})
    source = ColumnDataSource(data)
    x_range = Range1d(-0.5, 5.5)
    y_range = Range1d(-1.55, 0.4)

    inner, outer = 0.2, 0.4

    fig = figure(x_range=x_range, y_range=y_range, **kwargs)
    fig.annular_wedge(source=source, x='x', y='y',
                      inner_radius=inner, outer_radius=outer,
                      start_angle=90, end_angle=90.001,
                      direction='clock', color='#444444', alpha=0.1,
                      start_angle_units='deg', end_angle_units='deg')
    fig.annular_wedge(source=source, x='x', y='y',
                      inner_radius=inner, outer_radius=outer,
                      start_angle=90, end_angle='released-angle',
                      direction='clock', color=Spectral9[0], alpha=0.3,
                      start_angle_units='deg', end_angle_units='deg')
    fig.annular_wedge(source=source, x='x', y='y',
                      inner_radius=inner, outer_radius=outer,
                      start_angle='released-angle', end_angle='memory-angle',
                      direction='clock', color=Spectral9[0], alpha=0.8,
                      start_angle_units='deg', end_angle_units='deg')
    fig.annular_wedge(source=source, x='x', y='y',
                      inner_radius=inner, outer_radius=outer,
                      start_angle='memory-angle', end_angle='erred-angle',
                      direction='clock', color='#000000', alpha=0.3,
                      start_angle_units='deg', end_angle_units='deg')
    fig.text(source=source, x='x', y='y', text_align='center',
             text_baseline='bottom', text='done')
    fig.text(source=source, x='x', y='y', text_align='center',
             text_baseline='top', text='all')
    fig.text(source=source, x='x', y='lower-y', text_align='center',
             text='name')

    fig.xaxis.visible = False
    fig.yaxis.visible = False
    fig.grid.grid_line_alpha = 0

    hover = HoverTool()
    fig.add_tools(hover)
    hover = fig.select(HoverTool)
    hover.tooltips = """
    <div>
        <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">All:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@all</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">In Memory:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@memory</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">Erred:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@erred</span>
    </div>
    """
    hover.point_policy = 'follow_mouse'

    return source, fig
