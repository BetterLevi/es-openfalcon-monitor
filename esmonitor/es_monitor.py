import os
import json
import requests
import elasticsearch
from loguru import logger
from _datetime import datetime
from configparser import ConfigParser


class Monitor(object):
    status_map = {
        'green': 0,
        'yellow': 1,
        'red': 2
    }

    def __init__(self):
        self.es_clusters = ['es-cluster-us', 'es-cluster-hub']
        self.index_metrics = {
            'search': ['query_total', 'query_time_in_millis', 'query_current',
                       'fetch_total', 'fetch_time_in_millis', 'fetch_current'],
            'indexing': ['index_total', 'index_current', 'index_time_in_millis',
                         'delete_total', 'delete_current', 'delete_time_in_millis'],
            'docs': ['count', 'deleted'],
            'store': ['size_in_bytes'],
            'flush': ['total', 'total_time_in_millis']
        }
        self.net_metrics = ['current_open', 'total_opened']
        self.os_metrics = {
            'mem': ['heap_used_percent', 'heap_max_in_bytes']
        }
        self.cluster_metrics = ['status', 'number_of_nodes',
                                'number_of_data_nodes', 'active_primary_shards',
                                'active_shards', 'unassigned_shards']
        self.counter_keywords = ['query_total', 'query_time_in_millis',
                                 'fetch_total', 'fetch_time_in_millis',
                                 'index_total', 'index_time_in_millis',
                                 'delete_total', 'delete_time_in_millis']


def get_index_metrics(es_host='es-cluster-us'):
    index_metrics = Monitor().index_metrics
    config = load_config()
    es_host_address = config.get("elastic search", es_host)
    es_conn = elasticsearch.Elasticsearch(es_host_address)
    node_stat = es_conn.nodes.stats()
    metric_counter = {}
    for node in node_stat['nodes']:
        index_stat = node_stat['nodes'][node]['indices']
        for dimension in index_metrics:
            for metric in index_metrics[dimension]:
                metric_name = format_metric_name(dimension, metric)
                if metric not in metric_counter:
                    metric_counter[metric_name] = 0
                metric_counter[metric_name] += index_stat[dimension][metric]
    return metric_counter


def format_metric_name(metric_dimension, metric_name):
    return metric_dimension + '.' + metric_name


def get_cluster_metrics(es_host='es-cluster-us'):
    cluster_health_metrics = Monitor().cluster_metrics
    cluster_status_map = Monitor().status_map
    config = load_config()
    es_host_address = config.get("elastic search", es_host)
    es_conn = elasticsearch.Elasticsearch(es_host_address)
    cluster_health = es_conn.cluster.health()
    metric_counter = {}
    for metric in cluster_health_metrics:
        if metric == 'status':
            metric_counter['%s.%s' % (es_host, metric)] = cluster_status_map[cluster_health[metric]]
        else:
            metric_counter['%s.%s' % (es_host, metric)] = cluster_health[metric]
    return metric_counter


def get_os_metrics(cluster_name='es-cluster-us'):
    os_metrics = Monitor().os_metrics
    config = load_config()
    es_host_address = config.get('elastic search', cluster_name)
    es_conn = elasticsearch.Elasticsearch(es_host_address)
    node_stat = es_conn.nodes.stats()
    metric_counter = {}
    for node in node_stat['nodes']:
        jvm_stat = node_stat['nodes'][node]['jvm']
        for dimension in os_metrics:
            for metric in os_metrics[dimension]:
                metric_counter['%s.%s' % (metric, node_stat['nodes'][node]['name'])] = jvm_stat[dimension][metric]
    logger.debug("os metrics %s" % metric_counter)
    return metric_counter


def get_net_metrics(cluster_name='es-cluster-us'):
    net_metrics = Monitor().net_metrics
    config = load_config()
    es_host_address = config.get('elastic search', cluster_name)
    es_conn = elasticsearch.Elasticsearch(es_host_address)
    node_stat = es_conn.nodes.stats()
    metric_counter = {}
    for node in node_stat['nodes']:
        index_stat = node_stat['nodes'][node]['http']
        for metric in net_metrics:
            if metric not in metric_counter:
                metric_counter['http.%s' % metric] = 0
            metric_counter['http.%s' % metric] += index_stat[metric]
    return metric_counter


def format_falcon_data(metric, value, counter_type, tags, endpoint, timestamp, step):
    return {
        'metric': metric,
        'counterType': counter_type,
        'value': value,
        'tags': tags,
        'endpoint': endpoint,
        'timestamp': timestamp,
        'step': step,
    }


def load_config(config_file_name='monitor.cfg'):
    if not os.path.isfile(config_file_name):
        raise Exception('Failed to open config file')
    config = ConfigParser()
    config.read(config_file_name)
    return config


def push_data_to_falcon(data):
    post_data_json = json.dumps(data, sort_keys=True, indent=4)
    resp = requests.post("http://127.0.0.1:1988/v1/push", data=post_data_json)
    if resp.status_code == 200:
        logger.info("upload metric data to falcon successfully")
    elif resp.status_code == 404:
        logger.error("open-falcon url doesn't exit")
    elif resp.status_code == 500:
        logger.error("Open-falcon server error, please check Open-falcon status")
    else:
        logger.error("upload data failed. resp: %s please check" % resp)


def get_current_timestamp():
    return int(datetime.now().timestamp())


def save_former_status_data(es_cluster, query_total, query_millis, fetch_total, fetch_millis,
                            index_total, index_millis, flush_total, flush_millis):
    former_data = {"queryTotal": query_total, "queryMillis": query_millis,
                   "fetchTotal": fetch_total, "fetchMillis": fetch_millis,
                   "indexTotal": index_total, "indexMillis": index_millis,
                   "flushTotal": flush_total, "flushMillis": flush_millis}
    with open("%s_former_status_data.json" % es_cluster, "w", encoding="UTF8") as file:
        json.dump(former_data, file, ensure_ascii=False)


def calc_latency(cluster_name, query_total, query_time_in_millis, fetch_total, fetch_time_in_mills,
                 index_total, index_time_in_millis, flush_total, flush_time_in_millis):
    falcon_data = []
    with open("%s_former_status_data.json" % cluster_name, "r", encoding="UTF8") as file:
        former_data = json.load(file)
    delta_query_total = query_total - former_data["queryTotal"]
    delta_query_time = query_time_in_millis - former_data["queryMillis"]
    query_latency = round(delta_query_time / delta_query_total, 3) if delta_query_time else 0
    delta_fetch_total = fetch_total - former_data["fetchTotal"]
    delta_fetch_time = fetch_time_in_mills - former_data["fetchMillis"]
    fetch_latency = round(delta_fetch_time / delta_fetch_total, 3) if delta_fetch_time else 0
    delta_index_total = index_total - former_data["indexTotal"]
    delta_index_time = index_time_in_millis - former_data["indexMillis"]
    index_latency = round(delta_index_time / delta_index_total) if delta_index_time else 0
    delta_flush_total = flush_total - former_data["flushTotal"]
    delta_flush_time = flush_time_in_millis - former_data["flushMillis"]
    flush_latency = round(delta_flush_time / delta_flush_total, 3) if delta_flush_time else 0
    falcon_data.append(format_falcon_data("query_latency_ms", query_latency, "GAUGE", '',
                                          cluster_name, get_current_timestamp(), 60))
    falcon_data.append(format_falcon_data("fetch_latency_ms", fetch_latency, "GAUGE", '',
                                          cluster_name, get_current_timestamp(), 60))
    falcon_data.append(format_falcon_data("index_latency_ms", index_latency, "GAUGE", '',
                                          cluster_name, get_current_timestamp(), 60))
    falcon_data.append(format_falcon_data("flush_latency_ms", flush_latency, "GAUGE", '',
                                          cluster_name, get_current_timestamp(), 60))
    logger.debug("latency:%s" % falcon_data)
    return falcon_data


def main():
    es_clusters = Monitor().es_clusters
    falcon_data = []
    for cluster in es_clusters:
        metric_value = {}
        metric_value.update(get_index_metrics(cluster))
        metric_value.update(get_cluster_metrics(cluster))
        metric_value.update(get_net_metrics(cluster))
        metric_value.update(get_os_metrics(cluster))
        for key in metric_value:
            counter_type = 'COUNTER' if key in Monitor().counter_keywords else 'GAUGE'
            time_stamp = get_current_timestamp()
            falcon_data.append(format_falcon_data(key, metric_value[key], counter_type, '', cluster, time_stamp, 60))
        if os.path.exists("%s_former_status_data.json" % cluster):
            latency_data = calc_latency(cluster,
                                        metric_value['search.query_total'], metric_value['search.query_time_in_millis'],
                                        metric_value['search.fetch_total'], metric_value['search.fetch_time_in_millis'],
                                        metric_value['indexing.index_total'], metric_value['indexing.index_time_in_millis'],
                                        metric_value['flush.total'], metric_value['flush.total_time_in_millis']
                                        )
            for data in latency_data:
                falcon_data.append(data)
        save_former_status_data(cluster,
                                metric_value['search.query_total'], metric_value['search.query_time_in_millis'],
                                metric_value['search.fetch_total'], metric_value['search.fetch_time_in_millis'],
                                metric_value['indexing.index_total'], metric_value['indexing.index_time_in_millis'],
                                metric_value['flush.total'], metric_value['flush.total_time_in_millis'])
    logger.debug("push data to falcon:%s" % falcon_data)
    push_data_to_falcon(falcon_data)


if __name__ == '__main__':
    main()
    # get_os_metrics()
    # get_cluster_all_indices()
    # save_former_status_data("es-cluster-hub", 100, 1, 200, 0)
