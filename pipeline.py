import apache_beam as beam
from apache_beam.transforms.window import (
    TimestampedValue,
    Sessions,
    Duration,
    SlidingWindows
)
from apache_beam.io.textio import WriteToText, ReadAllFromText
from numpy import average
import json
from datetime import datetime
from dateutil import parser
import time

# User defined functions should always be subclassed from DoFn. This function transforms
# each element into a tuple where the first field is userId and the second is click. It
# assigns the timestamp to the metadata of the element such that window functions can use
# it later to group elements into windows.
class AddTimestampDoFn(beam.DoFn):
    def process(self, element):

        datetime_object = parser.parse(element['timestamp'])
        unix_timestamp = time.mktime(datetime_object.timetuple())
        element = (element["deviceId"], element["sensor1value"])
        yield TimestampedValue(element, unix_timestamp)


def mean_vals(values):
  print(values)
  max_value = 8
  return min(sum(values), max_value)

def transform_numbers(element):
    print(element)
    element['sensor1value'] = float(element['sensor1value'])
    element['sensor2Value'] = float(element['sensor2Value'])
    yield element

with beam.Pipeline() as p:
    
    # fmt: off
    events = p | beam.Create(["./tempdata.json"]) | ReadAllFromText()
    
    # fmt: on

    parsed_events = events | "Decode" >> beam.Map(lambda x: json.loads(x))
    mapped_events = parsed_events | "ParseNumbers" >> beam.Map(transform_numbers)
    # Assign timestamp to metadata of elements such that Beam's window functions can
    # access and use them to group events.
    timestamped_events = parsed_events | "AddTimestamp" >> beam.ParDo(AddTimestampDoFn())

    windowed_events = timestamped_events | beam.WindowInto(
        # Each session must be separated by a time gap of at least 30 minutes (1800 sec)
        SlidingWindows(60, 300),
        # Triggers determine when to emit the aggregated results of each window. Default
        # trigger outputs the aggregated result when it estimates all data has arrived,
        # and discards all subsequent data for that window.
        trigger=None,
        # Since a trigger can fire multiple times, the accumulation mode determines
        # whether the system accumulates the window panes as the trigger fires, or
        # discards them.
        accumulation_mode=None,
        # Policies for combining timestamps that occur within a window. Only relevant if
        # a grouping operation is applied to windows.
        timestamp_combiner=None,
        # By setting allowed_lateness we can handle late data. If allowed lateness is
        # set, the default trigger will emit new results immediately whenever late
        # data arrives.
        allowed_lateness=Duration(seconds=1 * 24 * 60 * 60),  # 1 day
    )

    # We can use CombinePerKey with the predifined sum function to combine all elements 
    # for each key in a collection.
    sum_clicks = windowed_events | beam.CombinePerKey(mean_vals)

    # WriteToText writes a simple text file with the results.
    sum_clicks | WriteToText(file_path_prefix="output")