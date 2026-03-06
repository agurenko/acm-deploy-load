#!/usr/bin/env python3
#
# Query and graph Prometheus data from a local Prometheus instance (hypervisor metrics)
#
#  Copyright 2023 Red Hat
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import argparse
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import pandas as pd
import plotly.express as px
import urllib3
import requests
import sys
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s : %(levelname)s : %(threadName)s : %(message)s")
logger = logging.getLogger("acm-deploy-load")
logging.Formatter.converter = time.gmtime


def calculate_query_offset(end_ts):
  cur_utc_unix_time = time.mktime(datetime.now(tz=timezone.utc).timetuple())
  offset_minutes = (int(cur_utc_unix_time) - end_ts) / 60
  if offset_minutes < 1:
    offset_minutes = 0
  return offset_minutes


def make_report_directories(sub_report_dir):
  csv_dir = os.path.join(sub_report_dir, "csv")
  stats_dir = os.path.join(sub_report_dir, "stats")
  if not os.path.exists(sub_report_dir):
    os.mkdir(sub_report_dir)
  if not os.path.exists(csv_dir):
    os.mkdir(csv_dir)
  if not os.path.exists(stats_dir):
    os.mkdir(stats_dir)


def short_hostname(instance_str):
  """Return short hostname (no FQDN, no port) for use in graph legends. e.g. 'host1.example.com:9100' -> 'host1'."""
  if not instance_str or not isinstance(instance_str, str):
    return instance_str
  host = instance_str.split(":")[0]
  return host.split(".")[0]


def query_prometheus(base_url, query, series_label, start_ts, end_ts, directory, fname, g_title, y_unit, g_width, g_height, q_names, step="60"):
  """Query local Prometheus via query_range, store CSV/stats, and plot. Mirrors query_thanos from analyze-prometheus.py."""
  logger.info("Querying data for {}".format(fname))
  if fname in q_names:
    logger.error("Query name already exists")
    sys.exit(1)
  q_names[fname] = g_title

  if y_unit == "CPU":
    y_title = "CPU (Cores)"
  elif y_unit == "MEM":
    y_title = "Memory (GiB)"
  elif y_unit == "NET":
    y_title = "Network (MiB)"
  elif y_unit == "DISK":
    y_title = "Disk (GB)"
  elif y_unit == "DISK_MB":
    y_title = "Disk (MB)"
  else:
    y_title = y_unit

  # Use query_range API for time series (start, end, step). Prometheus expects Unix timestamps in seconds.
  logger.info("Query: {} (start={}, end={}, step={}s)".format(query, start_ts, end_ts, step))
  query_endpoint = "{}/api/v1/query_range".format(base_url.rstrip("/"))
  payload = {"query": query, "start": start_ts, "end": end_ts, "step": step}
  query_data = requests.post(query_endpoint, data=payload, verify=False, timeout=120)

  if query_data.status_code == 200:
    qd_json = query_data.json()
    if qd_json.get("status") != "success":
      logger.warning("Prometheus returned status %r: %s", qd_json.get("status"), qd_json.get("error", ""))
    if "error" in qd_json and qd_json.get("errorType"):
      logger.warning("Prometheus error: %s - %s", qd_json.get("errorType", ""), qd_json.get("error", ""))
    if ("data" in qd_json) and ("result" in qd_json["data"]):
      logger.debug("Length of returned result data: {}".format(len(qd_json["data"]["result"])))

      if len(qd_json["data"]["result"]) == 0:
        logger.warning("Empty data returned from query (start=%s end=%s). Check that Prometheus has data in this time range. Response status: %s",
                       start_ts, end_ts, qd_json.get("status", "?"))
        if qd_json.get("data", {}).get("result") is not None:
          logger.debug("Full response: %s", json.dumps(qd_json, indent=2))
      else:
        # Build list of DataFrames, one per metric, then merge them (query_range returns "values")
        dfs = []
        series = []

        for metric in qd_json["data"]["result"]:
          points = metric.get("values") or ([metric["value"]] if "value" in metric else [])
          if not points:
            continue
          # Create datetime series for this metric
          metric_datetime = [datetime.fromtimestamp(x[0], tz=timezone.utc) for x in points]

          # Determine the series name (use short hostname for instance so legend is readable)
          if series_label not in metric["metric"]:
            metric_name = series_label
            logger.debug("Num of values: {}".format(len(points)))
          else:
            metric_name = metric["metric"][series_label]
            if series_label == "instance":
              metric_name = short_hostname(metric_name)
            logger.debug("{}: {}, Num of values: {}".format(series_label, metric_name, len(points)))

          # Convert values based on unit
          if y_unit == "MEM":
            bytes_to_gib = 1024 * 1024 * 1024
            metric_values = [float(x[1]) / bytes_to_gib for x in points]
          elif y_unit == "NET":
            bytes_to_mib = 1024 * 1024
            metric_values = [float(x[1]) / bytes_to_mib for x in points]
          elif y_unit == "DISK":
            bytes_to_gb = 1000 * 1000 * 1000
            metric_values = [float(x[1]) / bytes_to_gb for x in points]
          elif y_unit == "DISK_MB":
            bytes_to_mb = 1000 * 1000
            metric_values = [float(x[1]) / bytes_to_mb for x in points]
          else:
            metric_values = [float(x[1]) for x in points]

          # Create a DataFrame for this metric
          metric_df = pd.DataFrame({
            "datetime": metric_datetime,
            metric_name: metric_values
          })
          dfs.append(metric_df)
          series.append(metric_name)

        # Merge all DataFrames on datetime using outer join to handle different lengths
        df = dfs[0]
        for metric_df in dfs[1:]:
          df = pd.merge(df, metric_df, on="datetime", how="outer")
        # Sort by datetime after merge if we merged multiple DataFrames
        if len(dfs) > 1:
          df = df.sort_values("datetime").reset_index(drop=True)

        # Ensure datetime column is properly formatted for plotly
        if "datetime" in df.columns:
          df["datetime"] = pd.to_datetime(df["datetime"])

        # Filter series list to only include columns that actually exist in the DataFrame
        series_to_plot = [s for s in series if s in df.columns]

        if len(series_to_plot) == 0:
          logger.warning("No valid series to plot after merge")
        else:
          csv_dir = os.path.join(directory, "csv")
          stats_dir = os.path.join(directory, "stats")

          # Write graph and stats file
          with open("{}/{}.stats".format(stats_dir, fname), "a") as stats_file:
            with pd.option_context("display.max_columns", None, "display.width", 240):
              stats_file.write(str(df.describe(percentiles=[.25, .5, .75, .95, .99])))
          df.to_csv("{}/{}.csv".format(csv_dir, fname))

          # Create a copy for plotting with datetime as string to avoid plotly conversion issues
          df_plot = df.copy()
          if "datetime" in df_plot.columns:
            df_plot["datetime"] = df_plot["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

          l = {"value": y_title, "datetime": "Time (UTC)"}
          fig_cluster_node = px.line(df_plot, x="datetime", y=series_to_plot, labels=l, width=g_width, height=g_height)
          fig_cluster_node.update_layout(title=g_title, legend_orientation="v")
          fig_cluster_node.write_image("{}/{}.png".format(directory, fname))

      logger.info("Completed querying and graphing data")

    else:
      logger.error("Missing data/results field(s) from query result: {}".format(qd_json))
  else:
    logger.error("Query Post status returned: {}".format(query_data.status_code))
    logger.error("Query response: \n{}".format(query_data.text.rstrip()))


def hypervisor_queries(report_dir, base_url, start_ts, end_ts, step, w, h):
  """Query hypervisor (node exporter) metrics from local Prometheus."""
  sub_report_dir = os.path.join(report_dir, "hypervisor")
  make_report_directories(sub_report_dir)
  q_names = OrderedDict()

  # CPU utilization: 1 - idle rate, by instance (hypervisor)
  q = 'sum(1 - rate(node_cpu_seconds_total{mode="idle"}[1m])) by (instance)'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-cpu-util", "Hypervisor CPU utilization (1 - idle)", "CPU", w, h, q_names, step=step)

  # Memory: Total bytes by instance
  q = "node_memory_MemTotal_bytes"
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-mem-total", "Hypervisor memory total (GiB)", "MEM", w, h, q_names, step=step)

  # Network receive/transmit by device (rate in bytes/s, plotted as MiB/s)
  q = 'rate(node_network_receive_bytes_total{device="eno1"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-net-rcv-eno1", "Hypervisor network receive eno1 (MiB/s)", "NET", w, h, q_names, step=step)
  q = 'rate(node_network_receive_bytes_total{device="eno3"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-net-rcv-eno3", "Hypervisor network receive eno3 (MiB/s)", "NET", w, h, q_names, step=step)
  q = 'rate(node_network_transmit_bytes_total{device="eno1"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-net-xmt-eno1", "Hypervisor network transmit eno1 (MiB/s)", "NET", w, h, q_names, step=step)
  q = 'rate(node_network_transmit_bytes_total{device="eno3"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-net-xmt-eno3", "Hypervisor network transmit eno3 (MiB/s)", "NET", w, h, q_names, step=step)

  # Disk: IO now (rate of in-flight I/O)
  q = 'rate(node_disk_io_now{device="sda"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-disk-io-now-sda", "Hypervisor disk IO now sda", "Count", w, h, q_names, step=step)
  q = 'rate(node_disk_io_now{device="nvme0n1"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-disk-io-now-nvme0n1", "Hypervisor disk IO now nvme0n1", "Count", w, h, q_names, step=step)

  # Disk: Read rate (bytes/s -> MB/s)
  q = 'rate(node_disk_read_bytes_total{device="sda"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-disk-read-sda", "Hypervisor disk read sda (MB/s)", "DISK_MB", w, h, q_names, step=step)
  q = 'rate(node_disk_read_bytes_total{device="nvme0n1"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-disk-read-nvme0n1", "Hypervisor disk read nvme0n1 (MB/s)", "DISK_MB", w, h, q_names, step=step)

  # Disk: Write rate (bytes/s -> MB/s)
  q = 'rate(node_disk_written_bytes_total{device="sda"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-disk-write-sda", "Hypervisor disk write sda (MB/s)", "DISK_MB", w, h, q_names, step=step)
  q = 'rate(node_disk_written_bytes_total{device="nvme0n1"}[1m])'
  query_prometheus(base_url, q, "instance", start_ts, end_ts, sub_report_dir,
                   "hv-disk-write-nvme0n1", "Hypervisor disk write nvme0n1 (MB/s)", "DISK_MB", w, h, q_names, step=step)

  return q_names


def generate_report_html(report_dir, report_data):
  logger.info("Generating report html file")
  with open("{}/report.html".format(report_dir), "w") as html_file:
    html_file.write("<html>\n")
    html_file.write("<head><title>Hypervisor Prometheus Analysis Report</title></head>")
    html_file.write("<body>\n")
    html_file.write("<b>Hypervisor Prometheus Analysis Report</b><br>\n")
    for i, (section, v) in enumerate(report_data.items()):
      if i == len(report_data) - 1:
        html_file.write("<a href='#{0}'>{0} section</a>\n".format(section))
      else:
        html_file.write("<a href='#{0}'>{0} section</a> | \n".format(section))
    for section in report_data:
      html_file.write("<h2 id='{0}'>{0} section</h2>\n".format(section))
      for dp in report_data[section]:
        html_file.write("{} - {} | \n".format(report_data[section][dp], dp))
        html_file.write("<a href='{0}/{1}.png'>graph</a> | \n".format(section, dp))
        html_file.write("<a href='{0}/stats/{1}.stats'>stats</a> | \n".format(section, dp))
        html_file.write("<a href='{0}/csv/{1}.csv'>csv</a><br>\n".format(section, dp))
        html_file.write("<a href='{0}/{1}.png'><img src='{0}/{1}.png' width='700' height='500'></a><br>\n".format(section, dp))
    html_file.write("</body>\n")
    html_file.write("</html>\n")
  logger.info("Finished generating report html file")


def valid_datetime(datetime_arg):
  try:
    return datetime.strptime(datetime_arg, "%Y-%m-%dT%H:%M:%SZ")
  except ValueError:
    raise argparse.ArgumentTypeError("Datetime ({}) not valid! Expected format, 'YYYY-MM-DDTHH:mm:SSZ'!".format(datetime_arg))


def main():
  # Start time of script (Now)
  start_time = time.time()
  default_ap_end_time = datetime.fromtimestamp(start_time, tz=timezone.utc)
  default_ap_start_time = datetime.fromtimestamp(start_time - (60 * 60), tz=timezone.utc)

  parser = argparse.ArgumentParser(
      description="Query and graph Prometheus hypervisor (node) metrics from a local Prometheus instance",
      prog="analyze-hv.py", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  parser.add_argument("-u", "--prometheus-url", type=str, default="http://localhost:9090",
                      help="Base URL of local Prometheus (e.g. http://localhost:9090)")

  parser.add_argument("-s", "--start-ts", type=valid_datetime, default=default_ap_start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                      help="Sets start utc timestamp")
  parser.add_argument("-e", "--end-ts", type=valid_datetime, default=default_ap_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                      help="Sets end utc timestamp")

  parser.add_argument("-b", "--buffer-minutes", type=int, default=5,
                      help="Buffers start/end time stamps of data selected in minutes")

  parser.add_argument("-p", "--prefix", type=str, default="hv", help="Sets directory name prefix for files")

  parser.add_argument("-w", "--width", type=int, default=1400, help="Sets width of all graphs")
  parser.add_argument("-t", "--height", type=int, default=1000, help="Sets height of all graphs")

  parser.add_argument("results_directory", type=str, help="The location to place graphs and stats files")

  parser.add_argument("-d", "--debug", action="store_true", default=False, help="Set log level debug")
  cliargs = parser.parse_args()

  if cliargs.debug:
    logger.setLevel(logging.DEBUG)
  logger.debug("CLI Args: {}".format(cliargs))

  logger.info("Analyze Hypervisor Prometheus")

  w = cliargs.width
  h = cliargs.height
  base_url = cliargs.prometheus_url.rstrip("/")

  # Set/Validate start and end timestamps for queries
  buffer_time = (cliargs.buffer_minutes * 60)
  logger.info("Buffer time set to: {} seconds".format(buffer_time))

  # Interpret start/end as UTC (Z suffix in defaults). time.mktime() would treat naive datetime as local time.
  start_utc = cliargs.start_ts.replace(tzinfo=timezone.utc) if cliargs.start_ts.tzinfo is None else cliargs.start_ts
  end_utc = cliargs.end_ts.replace(tzinfo=timezone.utc) if cliargs.end_ts.tzinfo is None else cliargs.end_ts
  q_start_ts = int(start_utc.timestamp()) - buffer_time
  q_end_ts = int(end_utc.timestamp()) + buffer_time
  logger.info("Start timestamp set: {} (Unix {})".format(cliargs.start_ts, q_start_ts))
  logger.info("End timestamp set: {} (Unix {})".format(cliargs.end_ts, q_end_ts))

  analyze_duration = q_end_ts - q_start_ts
  if analyze_duration <= (60 * 5):
    logger.error("Start/End timestamps are too close")
    sys.exit(1)
  q_duration = "{}m".format(int(analyze_duration / 60))
  step_seconds = 60  # 1m resolution for query_range
  logger.info("Examining duration {}s :: {}".format(analyze_duration, str(timedelta(seconds=analyze_duration))))
  logger.info("Query range step: {}s".format(step_seconds))

  # Create the results directories
  report_dir = os.path.join(cliargs.results_directory, "{}-{}".format(cliargs.prefix,
      datetime.fromtimestamp(start_time, tz=timezone.utc).strftime("%Y%m%d-%H%M%S")))
  logger.debug("Creating report directory: {}".format(report_dir))
  if not os.path.exists(report_dir):
    os.mkdir(report_dir)

  with open("{}/analysis".format(report_dir), "a") as report_file:
    report_file.write("Prometheus URL: {}\n".format(base_url))
    report_file.write("Start Time: {}\n".format(cliargs.start_ts))
    report_file.write("End Time: {}\n".format(cliargs.end_ts))
    report_file.write("Buffer time: {}s\n".format(buffer_time))
    report_file.write("Examining duration: {}s :: {}\n".format(analyze_duration, str(timedelta(seconds=analyze_duration))))
    report_file.write("Query range step: {}s\n".format(step_seconds))

  report_data = OrderedDict()
  report_data["hypervisor"] = hypervisor_queries(report_dir, base_url, q_start_ts, q_end_ts, step_seconds, w, h)

  generate_report_html(report_dir, report_data)

  end_time = time.time()
  logger.info("Took {}s".format(round(end_time - start_time, 1)))


if __name__ == "__main__":
  sys.exit(main())
