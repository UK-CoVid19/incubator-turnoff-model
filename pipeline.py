import apache_beam as beam
from apache_beam.transforms.window import (
    TimestampedValue,
    Sessions,
    Duration,
    SlidingWindows,    
)
from apache_beam.io.textio import WriteToText, ReadAllFromText
import numpy as np
import json
from datetime import datetime
from dateutil import parser
import time
import logging
from main import process_points
# User defined functions should always be subclassed from DoFn. This function transforms
# each element into a tuple where the first field is userId and the second is click. It
# assigns the timestamp to the metadata of the element such that window functions can use
# it later to group elements into windows.
class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        datetime_object = parser.parse(element['timestamp'])
        unix_timestamp = time.mktime(datetime_object.timetuple())
        element = (element["deviceId"], (element["sensor1value"], unix_timestamp))
        yield TimestampedValue(element, unix_timestamp)

class ProcessResultFn(beam.DoFn):

    stable_state = beam.transforms.userstate.ReadModifyWriteStateSpec(name='stable', coder=beam.coders.BooleanCoder(), )
    unstable_timestamp_state = beam.transforms.userstate.ReadModifyWriteStateSpec(name='timestamp', coder=beam.coders.FloatCoder())

    def process(self, element, stable=beam.DoFn.StateParam(stable_state), unstable_timestamp=beam.DoFn.StateParam(unstable_timestamp_state)):
        print('element is', element)
        key, value = element

        is_stable = stable.read() or True
        print(is_stable, "stable read")
        unstable_timestamp_val = unstable_timestamp.read()
        result = process_points(value, is_stable, unstable_timestamp_val, stable.write, unstable_timestamp.write)
        print(result, key, "Result!!")

        message = {"sensor": key, "stable": result }
        
        print(message)
        yield message


def encode_to_bytes(message): 
    return [json.dumps(message).encode()]

def transform_numbers(element):
    element['sensor1value'] = float(element['sensor1Value'])
    element['sensor2Value'] = float(element['sensor2Value'])
    return element

def create_output(element):
    element['sensor1value'] = float(element['sensor1Value'])
    element['sensor2Value'] = float(element['sensor2Value'])
    return element


def is_datapoint(message):
    return "connected" not in message.decode('utf-8')

def has_4_readings(message):
    sensor_name, readings = message
    print("length of message is ", len(readings))
    return len(readings) > 3

def is_alert(message):
    return message["stable"] != "Stable"
    
def run():

    options = beam.options.pipeline_options.PipelineOptions(
        streaming=True
    )
    runner = 'DirectRunner'

    with beam.Pipeline(runner, options=options) as p:
        
        # fmt: off
        # events = p | beam.Create(["./tempdata.json"]) | ReadAllFromText()
        

        events = p | beam.io.ReadFromPubSub(topic="projects/neon-vigil-312408/topics/labsensortopic")
        # fmt: on
        filtered_events = events | "Filter data points" >> beam.Filter(is_datapoint)
        parsed_events = filtered_events | "Decode" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
        mapped_events = parsed_events | "ParseNumbers" >> beam.Map(transform_numbers)
        # Assign timestamp to metadata of elements such that Beam's window functions can
        # access and use them to group events.
        timestamped_events = mapped_events | "AddTimestamp" >> beam.ParDo(AddTimestampDoFn())

        windowed_events = timestamped_events | beam.WindowInto(
            # Each session must be separated by a time gap of at least 30 minutes (1800 sec)
            SlidingWindows(300, 60),
            # Triggers determine when to emit the aggregated results of each window. Default
            # trigger outputs the aggregated result when it estimates all data has arrived,
            # and discards all subsequent data for that window.
            trigger=beam.transforms.trigger.AfterWatermark(),
            # Since a trigger can fire multiple times, the accumulation mode determines
            # whether the system accumulates the window panes as the trigger fires, or
            # discards them.
            accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
            # Policies for combining timestamps that occur within a window. Only relevant if
            # a grouping operation is applied to windows.
            timestamp_combiner=None,
            # By setting allowed_lateness we can handle late data. If allowed lateness is
            # set, the default trigger will emit new results immediately whenever late
            # data arrives.
        )

        par_windowed = windowed_events | "Combine Results" >> beam.GroupByKey() | beam.Filter(has_4_readings)
        processed_results = par_windowed | "Process Results" >> beam.ParDo(ProcessResultFn())
        filtered_alerts = processed_results | "Filter Alerts" >> beam.Filter(is_alert)
        encoded_results = filtered_alerts | "Encode messages" >> beam.ParDo(encode_to_bytes)
        encoded_results | "WriteToPubSub" >> beam.io.WriteToPubSub("projects/neon-vigil-312408/topics/sensoralerts")

        # We can use CombinePerKey with the predifined sum function to combine all elements 
        # for each key in a collection.
        # sum_clicks = par_windowed | beam.CombineValues | WriteToText(file_path_prefix="output")
        
        # mapped_results = sum_clicks | "Map Results" >> beam.ParDo(MapResultFn())
        # # WriteToText writes a simple text file with the results.
        # mapped_results | WriteToText(file_path_prefix="output")

    result = p.run()
    result.wait_until_finish()

TOPIC_PATH = "projects/neon-vigil-312408/topics/labsensortopic"

def run2(pubsub_topic):
    options = beam.options.pipeline_options.PipelineOptions(
        streaming=True
    )
    runner = 'DirectRunner'

    print("I reached before pipeline")

    with beam.Pipeline(runner, options=options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub topic" >> beam.io.ReadFromPubSub(topic=pubsub_topic)
            | "Writing to console" >> beam.Map(print)
        )

    print("I reached after pipeline")

    result = pipeline.run()
    result.wait_until_finish()





if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # run2(TOPIC_PATH)
    run()