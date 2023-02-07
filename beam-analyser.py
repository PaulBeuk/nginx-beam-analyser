import argparse
import logging
import json
import time
import threading

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import window
from dateutil.parser import parse

"""store ips that use many user agents """
top_user_agents = []
def set_user_agent_logs(log):
    lock = threading.Lock()
    with lock:
        top_user_agents.append(log)

def sorted_top_user_agents():
    top_user_agents.sort(key=lambda x: x[1], reverse=True)
    return top_user_agents

def first_top_user_agent_switcher():
    if len(top_user_agents)>0:
        return top_user_agents[0]
    else:
        return None

"""store logs that download a lot of data"""
top_data_requests = []
def set_top_data_requests(requests,max):
    lock = threading.Lock()
    with lock:
        top_data_requests.extend(requests)
        top_data_requests.sort(key=lambda x: x['http.response.bytes'], reverse=True)
        top_length = len(top_data_requests)
        if( top_length > max):
            del top_data_requests[max:top_length]

def sorted_top_data_requests():
    return top_data_requests

"""set timestamp from log request timestamp"""
class AddTimestampFn(beam.DoFn):
    def process(self, element):
      try:
          timestamp = int(parse(element['@timestamp']).timestamp())
      except ValueError:
          timestamp = int(time.time())
      yield beam.window.TimestampedValue(element, timestamp)

"""format for user agent count"""
class FormatUserAgentFn(beam.DoFn):
    def process(self, element):
        element['http.response.bytes'] = int(element['http.response.bytes'])
        return [(element['source.ip'],element['user_agent.name'])]

"""set bytes to type int"""
class FormatDoFn(beam.DoFn):
    def process(self, element):
        element['http.response.bytes'] = int(element['http.response.bytes'])
        return [element]

"""sort by bytes"""
class SortDoFn(beam.DoFn):
    """Parse each line of input text into words."""
    def __init__(self):
        beam.DoFn.__init__(self)
        self.number_bytes_dist = Metrics.distribution(self.__class__, 'num_bytes_dist')
        self.empty_requests_counter = Metrics.counter(self.__class__, 'empty_requests')

    def process(self, element,max):
        (key, sort_data) = element
        sort_data.sort(key=lambda x: x['http.response.bytes'], reverse=True)

        count = 0
        total = 0
        top_list = []
        for x in sort_data:      
            count += 1
            if(count < max):
                top_list.append(x)

            total += x['http.response.bytes']
            self.number_bytes_dist.update(x['http.response.bytes'])
            if x['http.response.bytes'] == 0: 
                self.empty_requests_counter.inc()

        if(len(top_list)>0):
            set_top_data_requests(top_list,max)

"""when exceeding treshold store ip"""
class UserAgentFn(beam.DoFn):
    def process(self, element, treshold):
        (key, user_agents) = element
        user_agents = list(dict.fromkeys(user_agents))
        if len(user_agents) > treshold:
            set_user_agent_logs((key, len(user_agents), user_agents))

def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        required=True,
        help='Input file to process.')
    parser.add_argument(
        '--top_data_download',
        dest='top_data_download',
        default='10',
        help='Top data requests.')
    parser.add_argument(
        '--num_user_agents_tresholds',
        dest='user_agent_switch',
        default='3',
        help='Treshold user agent switch.')
      
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    # Read the text, format and add timestamp from log
    lines = (
        p 
        | 'read txtfile' >> ReadFromText(known_args.input)
        | 'parse json' >> beam.Map(json.loads)
        | 'format data' >> beam.ParDo(FormatDoFn())
        | 'Add timestamp' >> beam.ParDo(AddTimestampFn())
    )

    # group logs per second, sort and store top downloads 
    (
        lines 
        | 'set window' >> beam.WindowInto(window.FixedWindows(1 * 60))
        | 'add universal key' >> beam.Map(lambda line: (1, line))
        | 'group by key' >> beam.GroupByKey()
        | 'sorted by bytes' >> beam.ParDo(SortDoFn(),
            int(known_args.top_data_download))
    )
  

    # key by ip, group by ip and store the ips that use many user agents
    (
        lines 
        | 'tuple data for useragents count' >> beam.ParDo(FormatUserAgentFn())
        | 'Group counts per ip' >> beam.GroupByKey()
        | 'store ips that exceed the treshold' >> beam.ParDo(UserAgentFn(),
            int(known_args.user_agent_switch))
    )
    
    # start running the pipline and wait until finished
    result = p.run()
    result.wait_until_finish()

    # when ready and result has job, query and print the counters and distribution stats
    if (not hasattr(result, 'has_job') or result.has_job):
        empty_requests_filter = MetricsFilter().with_name('empty_requests')
        query_result = result.metrics().query(empty_requests_filter)
        if query_result['counters']:
            empty_requests_counter = query_result['counters'][0]
            print('\ncount stats')
            print(f'  number of zero bytes requests: {empty_requests_counter.result}')

        requests_lengths_filter = MetricsFilter().with_name('num_bytes_dist')
        query_result = result.metrics().query(requests_lengths_filter)
        if query_result['distributions']:
            number_bytes_dist = query_result['distributions'][0]

            print('\ndistribution stats')
            print(f'  number of requests: {number_bytes_dist.result.count}')
            print(f'  avg number of bytes downloaded: {number_bytes_dist.result.mean:.1f}')
            print(f'  max number of bytes downloaded: {number_bytes_dist.result.max}')

        print('\ntop user agent switchers')
        for x in sorted_top_data_requests():
            print(f'  {x["@timestamp"]} ip: {x["source.ip"]} bytes:{x["http.response.bytes"]} useragent: {x["user_agent.name"]} path {x["url.path"]}')

        print('\ntop user agent switchers')
        print(f'  first ip that exeeds the treshold: {first_top_user_agent_switcher()}')
        print('\nlist of top user agent switchers')
        for x in sorted_top_user_agents():
            print(f'  {x[0]} uses {x[1]} different user agents: {x[2]}')
    

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
