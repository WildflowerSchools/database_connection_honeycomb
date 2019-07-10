from database_connection import DatabaseConnection
from database_connection import DataQueue
import honeycomb
import datetime
import dateutil.parser
import json
import os

class DatabaseConnectionHoneycomb(DatabaseConnection):
    """
    Class to define a DatabaseConnection to Wildflower's Honeycomb database
    """
    def __init__(
        self,
        time_series_database = True,
        object_database = True,
        environment_name_honeycomb = None,
        object_type_honeycomb = None,
        object_id_field_name_honeycomb = None,
        honeycomb_uri = None,
        honeycomb_token_uri = None,
        honeycomb_audience = None,
        honeycomb_client_id = None,
        honeycomb_client_secret = None
    ):
        """
        Constructor for DatabaseConnectionHoneycomb.

        # If timestamp_field_name_input and timestamp_field_name_honeycomb are
        # specified, the database connection is assumed to be handling time series
        # data, every datapoint written to the database must contain a field with
        # the timestamp_field_name_input name, and this will enable various
        # time-related methods to access the data (e.g., fetching a time span,
        # creating an iterator that returns data points in time order).
        #
        # If object_id_field_name_input and object_id_field_name_honeycomb are
        # specified, the database is assumed to be handling data associated with
        # objects (e.g., measurement devices), every datapoint written to the
        # database must contain a field with the object_id_field_name_input name,
        # and this will enable various object methods to access the data (e.g.,
        # fetching all data associated with a specific list of object IDs).

        Parameters:
            time_series_database (bool): TBD
            object_database (bool): TBD
            environment_name_honeycomb (string): Name of the environment that the data should be associated with
            # timestamp_field_name_input (string): Name of the input field containing the timestamp for each datapoint
            # timestamp_field_name_honeycomb (string): Name of the Honeycomb field containing the timestamp for each datapoint
            # object_id_field_name_input (string): Name of the input field containing the object ID for each datapoint
            object_type_honeycomb (string): TBD
            object_id_field_name_honeycomb (string): TBD
            honeycomb_uri (string): TBD
            honeycomb_token_uri (string): TBD
            honeycomb_audience (string): TBD
            honeycomb_client_id (string): TBD
            honeycomb_client_secret (string): TBD
        """
        if not time_series_database and not object_database:
            raise ValueError('Database must be a time series database, an object database, or an object time series database')
        if time_series_database and object_database and environment_name_honeycomb is None:
            raise ValueError('Honeycomb environment name must be specified for object time series database')
        self.time_series_database = time_series_database
        self.object_database = object_database
        self.environment_name_honeycomb = environment_name_honeycomb
        self.object_type_honeycomb = object_type_honeycomb
        self.object_id_field_name_honeycomb = object_id_field_name_honeycomb
        if honeycomb_uri is None:
            honeycomb_uri = os.getenv('HONEYCOMB_URI')
            if honeycomb_uri is None:
                raise ValueError('Honeycomb URI not specified and environment variable HONEYCOMB_URI not set')
        if honeycomb_token_uri is None:
            honeycomb_token_uri = os.getenv('HONEYCOMB_TOKEN_URI')
            if honeycomb_token_uri is None:
                raise ValueError('Honeycomb token URI not specified and environment variable HONEYCOMB_TOKEN_URI not set')
        if honeycomb_audience is None:
            honeycomb_audience = os.getenv('HONEYCOMB_AUDIENCE')
            if honeycomb_audience is None:
                raise ValueError('Honeycomb audience not specified and environment variable HONEYCOMB_AUDIENCE not set')
        if honeycomb_client_id is None:
            honeycomb_client_id = os.getenv('HONEYCOMB_CLIENT_ID')
            if honeycomb_client_id is None:
                raise ValueError('Honeycomb client ID not specified and environment variable HONEYCOMB_CLIENT_ID not set')
        if honeycomb_client_secret is None:
            honeycomb_client_secret = os.getenv('HONEYCOMB_CLIENT_SECRET')
            if honeycomb_client_secret is None:
                raise ValueError('Honeycomb client secret not specified and environment variable HONEYCOMB_CLIENT_SECRET not set')
        self.honeycomb_client = honeycomb.HoneycombClient(
            uri = honeycomb_uri,
            client_credentials = {
                'token_uri': honeycomb_token_uri,
                'audience': honeycomb_audience,
                'client_id': honeycomb_client_id,
                'client_secret': honeycomb_client_secret,
            }
        )
        if self.environment_name_honeycomb is not None:
            environments = self.honeycomb_client.query.findEnvironment(name = self.environment_name_honeycomb)
            environment_id = environments.data[0].get('environment_id')
            self.environment = self.honeycomb_client.query.query(
                """
                query getEnvironment ($environment_id: ID!) {
                  getEnvironment(environment_id: $environment_id) {
                    environment_id
                    name
                    description
                    location
                    assignments {
                      assignment_id
                      start
                      end
                      assigned_type
                      assigned {
                        ... on Device {
                          device_id
                          part_number
                          name
                          tag_id
                          description
                        }
                        ... on Person {
                          person_id
                          name
                        }
                      }
                    }
                  }
                }
                """,
                {"environment_id": environment_id}).get("getEnvironment")

    def lookup_assignment_id_object_time_series(
        self,
        timestamp,
        object_id
    ):
        """
        Look up the Honeycomb assignment ID for a given timestamp and object ID.

        Parameters:
            # timestamp (string): Datetime at which we wish to know the assignment (as ISO-format string)
            object_id (string): Object ID for which we wish to know the assignment

        Returns:
            (string): Honeycomb assignment ID
        """
        if not self.time_series_database or not self.object_database or self.environment_name_honeycomb is None:
            raise ValueError('Assignment ID lookup only enabled for object time series databases with Honeycomb environment specified')
        for assignment in self.environment.get('assignments'):
            if assignment.get('assigned_type') != self.object_type_honeycomb:
                continue
            if assignment.get('assigned').get(self.object_id_field_name_honeycomb) != object_id:
                continue
            timestamp_datetime = python_datetime_utc(timestamp)
            start = assignment.get('start')
            if start is not None and timestamp_datetime < python_datetime_utc(start):
                continue
            end = assignment.get('end')
            if end is not None and timestamp_datetime > python_datetime_utc(end):
                continue
            return assignment.get('assignment_id')
        print('No assignment found for {} at {}'.format(
            object_id,
            timestamp
        ))
        return None

    def fetch_assignment_ids_object_time_series(
        self,
        start_time = None,
        end_time = None,
        object_ids = None
    ):
        if not self.time_series_database or not self.object_database or self.environment_name_honeycomb is None:
            raise ValueError('Assignment ID lookup only enabled for object time series databases with Honeycomb environment specified')
        relevant_assignment_ids = []
        for assignment in self.environment.get('assignments'):
            if assignment.get('assigned_type') != self.object_type_honeycomb:
                continue
            if object_ids is not None and assignment.get('assigned').get(self.object_id_field_name_honeycomb) not in object_ids:
                continue
            assignment_end = assignment.get('end')
            if start_time is not None and assignment_end is not None and python_datetime_utc(start_time) >  python_datetime_utc(assignment_end):
                continue
            assignment_start = assignment.get('start')
            if end_time is not None and assignment_start is not None and python_datetime_utc(end_time) <  python_datetime_utc(assignment_start):
                continue
            relevant_assignment_ids.append(assignment.get('assignment_id'))
        return relevant_assignment_ids

    def fetch_data_object_time_series(
        self,
        start_time = None,
        end_time = None,
        object_ids = None
    ):
        if not self.time_series_database or not self.object_database:
            raise ValueError('Fetching data by time interval and/or object ID only enabled for object time series databases')
        assignment_ids = self.fetch_assignment_ids_object_time_series(
            start_time,
            end_time,
            object_ids
        )
        query_expression_string = combined_query_expression_string(
            assignment_ids,
            start_time,
            end_time
        )
        query_string = fetch_data_object_time_series_query_string(query_expression_string)
        query_results = self.honeycomb_client.query.query(query_string, variables = {})
        data = []
        for query_results_datum in query_results.get('findDatapoints').get('data'):
            datum = {}
            datum.update({'timestamp': python_datetime_utc(query_results_datum.get('observed_time'))})
            datum.update({'environment_name': query_results_datum.get('observer', {}).get('environment', {}).get('name')})
            datum.update({'object_id': query_results_datum.get('observer', {}).get('assigned', {}).get(self.object_id_field_name_honeycomb)})
            data_string = query_results_datum.get('file', {}).get('data')
            try:
                data_dict = json.loads(data_string)
                datum.update(data_dict)
            except:
                pass
            data.append(datum)
        return data

    def write_data_object_time_series(
        self,
        timestamp,
        object_id,
        data_dict
    ):
        if not self.time_series_database or not self.object_database:
            raise ValueError('Writing data by timestamp and object ID only enabled for object time series databases')
        assignment_id = self.lookup_assignment_id_object_time_series(timestamp, object_id)
        timestamp_honeycomb_format = datetime_honeycomb_string(timestamp)
        data_json = json.dumps(data_dict)
        dp = honeycomb.models.DatapointInput(
                observer = assignment_id,
                format = 'application/json',
                file = honeycomb.models.S3FileInput(
                    name = 'datapoint.json',
                    contentType = 'application/json',
                    data = data_json,
                ),
                observed_time= timestamp_honeycomb_format
        )
        self.honeycomb_client.mutation.createDatapoint(dp)

def python_datetime_utc(timestamp):
    try:
        if timestamp.tzinfo is None:
            datetime_utc = timestamp.replace(tzinfo = datetime.timezone.utc)
        else:
            datetime_utc = timestamp.astimezone(tz=datetime.timezone.utc)
    except:
        datetime_parsed = dateutil.parser.parse(timestamp)
        if datetime_parsed.tzinfo is None:
            datetime_utc = datetime_parsed.replace(tzinfo = datetime.timezone.utc)
        else:
            datetime_utc = datetime_parsed.astimezone(tz=datetime.timezone.utc)
    return datetime_utc

def datetime_honeycomb_string(timestamp):
    datetime_utc = python_datetime_utc(timestamp)
    datetime_honeycomb_string = datetime_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return datetime_honeycomb_string

def query_expression_string(
    field_string = None,
    operator_string = None,
    value_string = None,
    children_query_expression_string_list = None
):
    query_expression_string = '{'
    if field_string is not None:
        query_expression_string += 'field: "{}", '.format(field_string)
    if operator_string is not None:
        query_expression_string += 'operator: {}, '.format(operator_string)
    if value_string is not None:
        query_expression_string += 'value: "{}"'.format(value_string)
    if children_query_expression_string_list is not None:
        query_expression_string += 'children: [{}]'.format(', '.join(children_query_expression_string_list))
    query_expression_string += '}'
    return query_expression_string

def assignment_ids_query_expression_string(assignment_ids):
    assignment_ids_query_expression_string_list = []
    for assignment_id in assignment_ids:
        assigment_id_query_expression_string = query_expression_string(
            field_string = 'observer',
            operator_string = 'EQ',
            value_string = assignment_id
        )
        assignment_ids_query_expression_string_list.append(assigment_id_query_expression_string)
    assignment_ids_query_expression_string = query_expression_string(
        operator_string = 'OR',
        children_query_expression_string_list = assignment_ids_query_expression_string_list
    )
    return assignment_ids_query_expression_string

def start_time_query_expression_string(start_time):
    start_time_honeycomb_string = datetime_honeycomb_string(start_time)
    start_time_query_expression_string = query_expression_string(
        field_string = 'observed_time',
        operator_string = 'GTE',
        value_string = start_time_honeycomb_string
    )
    return start_time_query_expression_string

def end_time_query_expression_string(end_time):
    end_time_honeycomb_string = datetime_honeycomb_string(end_time)
    end_time_query_expression_string = query_expression_string(
        field_string = 'observed_time',
        operator_string = 'LTE',
        value_string = end_time_honeycomb_string
    )
    return end_time_query_expression_string

def combined_query_expression_string(
    assignment_ids,
    start_time = None,
    end_time = None
):
    combined_query_expression_string_list = []
    assignment_ids_string = assignment_ids_query_expression_string(assignment_ids)
    combined_query_expression_string_list.append(assignment_ids_string)
    if start_time is not None:
        start_time_string = start_time_query_expression_string(start_time)
        combined_query_expression_string_list.append(start_time_string)
    if end_time is not None:
        end_time_string = end_time_query_expression_string(end_time)
        combined_query_expression_string_list.append(end_time_string)
    combined_query_expression_string = query_expression_string(
        operator_string = 'AND',
        children_query_expression_string_list = combined_query_expression_string_list
    )
    return combined_query_expression_string

fetch_data_object_time_series_query_string_template = """
query fetchDataTimeSeries {{
  findDatapoints(query: {}) {{
    data {{
      observed_time
      observer {{
        ... on Assignment {{
            environment {{
                name
            }}
            assigned {{
              ... on Device {{
                part_number
                tag_id
              }}
              ... on Person {{
                name
              }}
            }}
        }}
      }}
      file {{
        data
        name
        contentType
      }}
    }}
  }}
}}
"""

def fetch_data_object_time_series_query_string(query_expression_string):
    query_string = fetch_data_object_time_series_query_string_template.format(query_expression_string)
    return query_string
