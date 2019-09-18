from database_connection import DatabaseConnection
import honeycomb
from gqlpycgen.client import FileUpload
from uuid import uuid4
import json
import os
import math


class DatabaseConnectionHoneycomb(DatabaseConnection):
    """
    Class to define a DatabaseConnection to Wildflower's Honeycomb database
    """

    def __init__(
        self,
        time_series_database=True,
        object_database=True,
        environment_name_honeycomb=None,
        object_type_honeycomb=None,
        object_id_field_name_honeycomb=None,
        write_chunk_size=20,
        honeycomb_uri=None,
        honeycomb_token_uri=None,
        honeycomb_audience=None,
        honeycomb_client_id=None,
        honeycomb_client_secret=None
    ):
        """
        Constructor for DatabaseConnectionHoneycomb.

        If time_series_database and object_database are both True, database is
        an object time series database (e.g., a measurement database) and
        datapoints are identified by timestamp and object ID.

        If object_database is True and time_series_database is False, database
        is an object database (e.g., a device configuration database) and
        datapoints are identified by object ID.

        If time_series_database is True and object_database is False, behavior
        is not defined (for now).

        For an object time series database, Honeycomb environment, object type,
        and object ID field name must be specified.

        If Honeycomb access parameters (URI, token URI, audience, client ID,
        client secret) are not specified, method will attempt to read from
        corresponding environment variables (HONEYCOMB_URI, HONEYCOMB_TOKEN_URI,
        HONEYCOMB_AUDIENCE, HONEYCOMB_CLIENT_ID, HONEYCOMB_CLIENT_SECRET).

        Parameters:
            time_series_database (bool): Boolean indicating whether database is a time series database (default is True)
            object_database (bool): Boolean indicating whether database is an object database (default is True)
            environment_name_honeycomb (string): Name of the Honeycomb environment that the data is associated with
            object_type_honeycomb (string): Honeycomb object type that the data is associated with (e.g. DEVICE, PERSON)
            object_id_field_name_honeycomb (string): Honeycomb field name that holds the object ID (e.g., part_number)
            honeycomb_uri (string): Honeycomb URI
            honeycomb_token_uri (string): Honeycomb token URI
            honeycomb_audience (string): Honeycomb audience
            honeycomb_client_id (string): Honeycomb client ID
            honeycomb_client_secret (string): Honeycomb client secret
        """
        if not time_series_database and not object_database:
            raise ValueError('Database must be a time series database, an object database, or an object time series database')
        if time_series_database and object_database and environment_name_honeycomb is None:
            raise ValueError('Honeycomb environment name must be specified for object time series database')
        if time_series_database and object_database and object_type_honeycomb is None:
            raise ValueError('Honeycomb object type must be specified for object time series database')
        if time_series_database and object_database and object_id_field_name_honeycomb is None:
            raise ValueError('Honeycomb object ID field name must be specified for object time series database')
        self.time_series_database = time_series_database
        self.object_database = object_database
        self.environment_name_honeycomb = environment_name_honeycomb
        self.object_type_honeycomb = object_type_honeycomb
        self.object_id_field_name_honeycomb = object_id_field_name_honeycomb
        self.write_chunk_size = write_chunk_size
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
            uri=honeycomb_uri,
            client_credentials={
                'token_uri': honeycomb_token_uri,
                'audience': honeycomb_audience,
                'client_id': honeycomb_client_id,
                'client_secret': honeycomb_client_secret,
            }
        )
        if self.environment_name_honeycomb is not None:
            environments = self.honeycomb_client.query.findEnvironment(name=self.environment_name_honeycomb)
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
                          serial_number
                          mac_address
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

    # Internal method for writing a single datapoint of object time series data
    # (Honeycomb-specific)
    def _write_datapoint_object_time_series(
        self,
        timestamp,
        object_id,
        data
    ):
        assignment_id = self._lookup_assignment_id_object_time_series(timestamp, object_id)
        timestamp_honeycomb_format = self._datetime_honeycomb_string(timestamp)
        data_json = json.dumps(data)
        dp = honeycomb.models.DatapointInput(
            observer=assignment_id,
            format='application/json',
            file=honeycomb.models.S3FileInput(
                name='datapoint.json',
                contentType='application/json',
                data=data_json,
            ),
            observed_time=timestamp_honeycomb_format,
        )
        output = self.honeycomb_client.mutation.createDatapoint(dp)
        data_id = output.data_id
        return data_id

    # Internal method for writing object time series data (Honeycomb-specific)
    def _write_data_object_time_series(
        self,
        datapoints
    ):
        num_datapoints = len(datapoints)
        num_chunks = math.ceil(num_datapoints / self.write_chunk_size)
        data_ids = []
        for chunk_index in range(num_chunks):
            chunk_beginning = chunk_index * self.write_chunk_size
            chunk_end = min((chunk_index + 1) * self.write_chunk_size, num_datapoints)
            chunk_datapoints = datapoints[chunk_beginning:chunk_end]
            chunk_data_ids = self._write_datapoints_object_time_series(chunk_datapoints)
            data_ids.extend(chunk_data_ids)
        return data_ids

    # Internal method for writing multiple datapoints of object time series data
    # (Honeycomb-specific)
    def _write_datapoints_object_time_series(
        self,
        datapoints
    ):
        num_datapoints = len(datapoints)
        query = 'mutation createDatapoints ({}) {{\n{}\n}}'.format(
            ', '.join(['$datapoint_{}: DatapointInput'.format(i) for i in range(num_datapoints)]),
            '\n'.join(['    data_id_{}: createDatapoint(datapoint: $datapoint_{}) {{data_id}}'.format(i, i) for i in range(num_datapoints)])
        )
        variables = dict()
        files = FileUpload()
        for datapoint_index, datapoint_dict in enumerate(datapoints):
            timestamp = datapoint_dict.pop('timestamp')
            object_id = datapoint_dict.pop('object_id')
            assignment_id = self._lookup_assignment_id_object_time_series(timestamp, object_id)
            timestamp_honeycomb_format = self._datetime_honeycomb_string(timestamp)
            data_json = json.dumps(datapoint_dict)
            filename = uuid4().hex
            files.add_file(
                'variables.datapoint_{}.file.data'.format(datapoint_index),
                filename,
                data_json,
                'application/json'
            )
            datapoint_input_object = honeycomb.models.DatapointInput(
                observer=assignment_id,
                format='application/json',
                file=honeycomb.models.S3FileInput(
                    name='datapoint.json',
                    contentType='application/json',
                    data=filename,
                ),
                observed_time=timestamp_honeycomb_format
            )
            if hasattr(datapoint_input_object, "to_json"):
                variables['datapoint_{}'.format(datapoint_index)] = datapoint_input_object.to_json()
            else:
                variables['datapoint_{}'.format(datapoint_index)] = datapoint_input_object
        results = self.honeycomb_client.client.execute(query, variables, files)
        try:
            data_ids = [results['data_id_{}'.format(i)]['data_id'] for i in range(num_datapoints)]
        except Exception:
            raise Exception('Received unexpected response from Honeycomb')
        return data_ids

    # Internal method for fetching object time series data (Honeycomb-specific)
    def _fetch_data_object_time_series(
        self,
        start_time,
        end_time,
        object_ids
    ):
        datapoints = self._fetch_datapoints_object_time_series(
            start_time,
            end_time,
            object_ids
        )
        data = []
        for datapoint in datapoints:
            datum = {}
            datum.update({'timestamp': self._python_datetime_utc(datapoint.get('observed_time'))})
            datum.update({'environment_name': datapoint.get('observer', {}).get('environment', {}).get('name')})
            datum.update({'object_id': datapoint.get('observer', {}).get('assigned', {}).get(self.object_id_field_name_honeycomb)})
            data_string = datapoint.get('file', {}).get('data')
            try:
                data_dict = json.loads(data_string)
                datum.update(data_dict)
            except Exception:
                pass
            data.append(datum)
        return data

    # Internal method for deleting object time series data (Honeycomb-specific)
    def _delete_data_object_time_series(
        self,
        start_time,
        end_time,
        object_ids
    ):
        data_ids = self._fetch_data_ids_object_time_series(
            start_time,
            end_time,
            object_ids
        )
        self._delete_datapoints(data_ids)

    def _delete_datapoints(self, data_ids):
        statuses = [self._delete_datapoint(data_id) for data_id in data_ids]
        return statuses

    def _delete_datapoint(self, data_id):
        status = self.honeycomb_client.query.query(
            """
            mutation deleteSingleDatapoint ($data_id: ID!) {
              deleteDatapoint(data_id: $data_id) {
                status
              }
            }
            """,
            {"data_id": data_id}).get("deleteSingleDatapoint", {}).get('status')
        return status

    def _lookup_assignment_id_object_time_series(
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
            timestamp_datetime = self._python_datetime_utc(timestamp)
            start = assignment.get('start')
            if start is not None and timestamp_datetime < self._python_datetime_utc(start):
                continue
            end = assignment.get('end')
            if end is not None and timestamp_datetime > self._python_datetime_utc(end):
                continue
            return assignment.get('assignment_id')
        print('No assignment found for {} at {}'.format(
            object_id,
            timestamp
        ))
        return None

    def _fetch_assignment_ids_object_time_series(
        self,
        start_time=None,
        end_time=None,
        object_ids=None
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
            if start_time is not None and assignment_end is not None and self._python_datetime_utc(start_time) > self._python_datetime_utc(assignment_end):
                continue
            assignment_start = assignment.get('start')
            if end_time is not None and assignment_start is not None and self._python_datetime_utc(end_time) < self._python_datetime_utc(assignment_start):
                continue
            relevant_assignment_ids.append(assignment.get('assignment_id'))
        return relevant_assignment_ids

    def _fetch_data_ids_object_time_series(
        self,
        start_time=None,
        end_time=None,
        object_ids=None
    ):
        if not self.time_series_database or not self.object_database:
            raise ValueError('Fetching data IDs by time interval and/or object ID only enabled for object time series databases')
        datapoints = self._fetch_datapoints_object_time_series(
            start_time,
            end_time,
            object_ids
        )
        data_ids = []
        for datapoint in datapoints:
            data_ids.append(datapoint.get('data_id'))
        return data_ids

    def _fetch_datapoints_object_time_series(
        self,
        start_time=None,
        end_time=None,
        object_ids=None
    ):
        if not self.time_series_database or not self.object_database:
            raise ValueError('Fetching datapoints by time interval and/or object ID only enabled for object time series databases')
        assignment_ids = self._fetch_assignment_ids_object_time_series(
            start_time,
            end_time,
            object_ids
        )
        query_expression_string = self._combined_query_expression_string(
            assignment_ids,
            start_time,
            end_time
        )
        query_string = self._fetch_datapoints_object_time_series_query_string(query_expression_string)
        query_results = self.honeycomb_client.query.query(query_string, variables={})
        datapoints = query_results.get('findDatapoints').get('data')
        return datapoints

    def _datetime_honeycomb_string(self, timestamp):
        datetime_utc = self._python_datetime_utc(timestamp)
        datetime_honeycomb_string = datetime_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return datetime_honeycomb_string

    def _query_expression_string(
        self,
        field_string=None,
        operator_string=None,
        value_string=None,
        children_query_expression_string_list=None
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

    def _assignment_ids_query_expression_string(self, assignment_ids):
        assignment_ids_query_expression_string_list = []
        for assignment_id in assignment_ids:
            assigment_id_query_expression_string = self._query_expression_string(
                field_string='observer',
                operator_string='EQ',
                value_string=assignment_id
            )
            assignment_ids_query_expression_string_list.append(assigment_id_query_expression_string)
        assignment_ids_query_expression_string = self._query_expression_string(
            operator_string='OR',
            children_query_expression_string_list=assignment_ids_query_expression_string_list
        )
        return assignment_ids_query_expression_string

    def _start_time_query_expression_string(self, start_time):
        start_time_honeycomb_string = self._datetime_honeycomb_string(start_time)
        start_time_query_expression_string = self._query_expression_string(
            field_string='observed_time',
            operator_string='GTE',
            value_string=start_time_honeycomb_string
        )
        return start_time_query_expression_string

    def _end_time_query_expression_string(self, end_time):
        end_time_honeycomb_string = self._datetime_honeycomb_string(end_time)
        end_time_query_expression_string = self._query_expression_string(
            field_string='observed_time',
            operator_string='LTE',
            value_string=end_time_honeycomb_string
        )
        return end_time_query_expression_string

    def _combined_query_expression_string(
        self,
        assignment_ids,
        start_time=None,
        end_time=None
    ):
        combined_query_expression_string_list = []
        assignment_ids_string = self._assignment_ids_query_expression_string(assignment_ids)
        combined_query_expression_string_list.append(assignment_ids_string)
        if start_time is not None:
            start_time_string = self._start_time_query_expression_string(start_time)
            combined_query_expression_string_list.append(start_time_string)
        if end_time is not None:
            end_time_string = self._end_time_query_expression_string(end_time)
            combined_query_expression_string_list.append(end_time_string)
        combined_query_expression_string = self._query_expression_string(
            operator_string='AND',
            children_query_expression_string_list=combined_query_expression_string_list
        )
        return combined_query_expression_string

    def _fetch_datapoints_object_time_series_query_string(self, query_expression_string):
        string_template = """
            query fetchDataTimeSeries {{
              findDatapoints(query: {}) {{
                data {{
                  data_id
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
        query_string = string_template.format(query_expression_string)
        return query_string
