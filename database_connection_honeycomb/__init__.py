from database_connection import DatabaseConnection
import minimal_honeycomb
import json
import os
import math
import logging

logger = logging.getLogger(__name__)

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
        read_chunk_size=1000,
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
            write_chunk_size (int): Number of datapoints to write in each request (default is 20)
            read_chunk_size (int): Number of datapoints to read in each request (default is 1000)
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
        self.read_chunk_size = read_chunk_size
        self.honeycomb_client = minimal_honeycomb.MinimalHoneycombClient(
            uri=honeycomb_uri,
            token_uri=honeycomb_token_uri,
            audience=honeycomb_audience,
            client_id=honeycomb_client_id,
            client_secret=honeycomb_client_secret
        )
        if self.environment_name_honeycomb is not None:
            findEnvironment_result = self.honeycomb_client.request(
                request_type='query',
                request_name='findEnvironment',
                arguments= {
                    'name': {
                        'type': 'String',
                        'value': self.environment_name_honeycomb
                    }
                },
                return_object = [
                    {'data': [
                        'environment_id'
                    ]}
                ]
            )
            if len(findEnvironment_result.get('data')) == 0:
                raise ValueError('Environment name {} matched no environments'.format(self.environment_name_honeycomb))
            if len(findEnvironment_result.get('data')) > 1:
                raise ValueError('Environment name {} matched more than one environment'.format(self.environment_name_honeycomb))
            environment_id = findEnvironment_result.get('data')[0].get('environment_id')
            getEnvironment_result = self.honeycomb_client.request(
                request_type='query',
                request_name='getEnvironment',
                arguments={
                    'environment_id': {
                        'type': 'ID!',
                        'value': environment_id
                    }
                },
                return_object = [
                    'name',
                    {'assignments': [
                        'assignment_id',
                        'start',
                        'end',
                        'assigned_type',
                        {'assigned': [
                            {'... on Device': [
                                'device_id',
                                'device_type',
                                'part_number',
                                'serial_number',
                                'name',
                                'mac_address',
                                'tag_id'
                            ]},
                            {'... on Person': [
                                'person_id',
                                'name',
                                'first_name',
                                'last_name',
                                'nickname',
                                'short_name',
                                'person_type',
                                'transparent_classroom_id'
                            ]},
                            {'... on Material': [
                                'material_id',
                                'name',
                                'transparent_classroom_id'
                            ]},
                            {'... on Tray': [
                                'tray_id',
                                'part_number',
                                'name',
                                'serial_number'
                            ]}
                        ]}
                    ]}
                ]
            )
            self.environment = getEnvironment_result


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
        createDatapoint_result = self.honeycomb_client.request(
            request_type='mutation',
            request_name='createDatapoint',
            arguments={
                'datapoint': {
                    'type': 'DatapointInput',
                    'value': {
                        'timestamp': timestamp_honeycomb_format,
                        'format': 'application/json',
                        'source_type': 'MEASURED',
                        'source': assignment_id,
                        'file': {
                            'name': 'datapoint.json',
                            'contentType': 'application/json',
                            'data': data
                        }
                    }
                }
            },
            return_object = [
                'data_id'
            ]
        )
        data_id = createDatapoint_result.get('data_id')
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

    # Internal method for fetching object time series data (Honeycomb-specific)
    def _fetch_data_object_time_series(
        self,
        start_time,
        end_time,
        object_ids
    ):
        logger.info('Fetching datapoints between {} and {}'.format(
            start_time,
            end_time
        ))
        datapoints = self._fetch_datapoints_object_time_series(
            start_time,
            end_time,
            object_ids
        )
        logger.info('Parsing {} datapoints'.format(len(datapoints)))
        data=[]
        for datapoint in datapoints:
            source = datapoint.get('source')
            timestamp = self._python_datetime_utc(datapoint.get('timestamp'))
            environment_name = source.get('environment', {}).get('name')
            object_id = source.get('assigned', {}).get(self.object_id_field_name_honeycomb)
            base_data_dict = {
                'timestamp': timestamp,
                'environment_name': environment_name,
                'object_id': object_id
            }
            data_blob = datapoint.get('file', {}).get('data')
            extracted_data_dict_list = self.parse_data_blob(data_blob)
            for extracted_data_dict in extracted_data_dict_list:
                sanitized_extracted_data_dict = dict()
                for key, value in extracted_data_dict.items():
                    if key in base_data_dict.keys():
                        sanitized_extracted_data_dict[key + '_secondary'] = extracted_data_dict[key]
                    else:
                        sanitized_extracted_data_dict[key] = extracted_data_dict[key]
                complete_data_dict = {**base_data_dict, **sanitized_extracted_data_dict}
                data.append(complete_data_dict)
        return data

    # Internal method for parsing a data blob from Honeycomb into a list of dictionaries
    def parse_data_blob(
        self,
        data_blob
    ):
        data_dict_list=[]
        if isinstance(data_blob, dict):
            data_dict_list.append(data_blob)
            return data_dict_list
        if isinstance(data_blob, list):
            for item in data_blob:
                data_dict_list.extend(self.parse_data_blob(item))
            return data_dict_list
        try:
            data_dict_list.extend(self.parse_data_blob(json.loads(data_blob)))
            return data_dict_list
        except:
            pass
        try:
            for line in data_blob.split('\n'):
                if len(line) > 0:
                    data_dict_list.extend(self.parse_data_blob(line))
            return data_dict_list
        except:
            pass
        return data_dict_list

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

    # Internal method for writing multiple datapoints of object time series data
    # (Honeycomb-specific)
    def _write_datapoints_object_time_series(
        self,
        datapoints
    ):
        num_datapoints = len(datapoints)
        parent_request_type = 'mutation'
        parent_request_name = 'createDatapoints'
        child_request_list = []
        for datapoint_index, datapoint_dict in enumerate(datapoints):
            timestamp = datapoint_dict.pop('timestamp')
            object_id = datapoint_dict.pop('object_id')
            assignment_id = self._lookup_assignment_id_object_time_series(timestamp, object_id)
            timestamp_honeycomb_format = self._datetime_honeycomb_string(timestamp)
            child_request_name = 'createDatapoint'
            child_arguments = {
                'datapoint': {
                    'type': 'DatapointInput',
                    'value': {
                        'timestamp': timestamp_honeycomb_format,
                        'format': 'application/json',
                        'source_type': 'MEASURED',
                        'source': assignment_id,
                        'file': {
                            'name': 'datapoint.json',
                            'contentType': 'application/json',
                            'data': datapoint_dict
                        }
                    }
                }
            }
            child_return_object_name = 'data_id'
            child_return_object = [
                'data_id'
            ]
            child_request_list.append({
                'name': child_request_name,
                'arguments': child_arguments,
                'return_object_name': child_return_object_name,
                'return_object': child_return_object
            })
        createDatapoints_result = self.honeycomb_client.compound_request(
            parent_request_type=parent_request_type,
            parent_request_name=parent_request_name,
            child_request_list=child_request_list
        )
        try:
            data_ids = [createDatapoints_result['data_id_{}'.format(datapoint_index)]['data_id'] for datapoint_index in range(num_datapoints)]
        except:
            raise ValueError('Received unexpected response from Honeycomb: {}'.format(createDatapoints_result))
        return data_ids

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
        logger.warning('No assignment found for {} at {}'.format(
            object_id,
            timestamp
        ))
        return None

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
        if len(assignment_ids) == 0:
            return []
        query_expression = self._combined_query_expression(
            assignment_ids,
            start_time,
            end_time
        )
        datapoints = []
        chunk_counter = 1
        data_ids = set()
        cursor = None
        while True:
            arguments = self._fetch_datapoints_arguments(
                query_expression,
                cursor
            )
            searchDatapoints_result = self.honeycomb_client.request(
                request_type='query',
                request_name='searchDatapoints',
                arguments=arguments,
                return_object=FETCH_DATA_RETURN_OBJECT
            )
            count = searchDatapoints_result.get('page_info').get('count')
            cursor = searchDatapoints_result.get('page_info').get('cursor')
            if cursor is None or count == 0:
                break
            chunk_datapoints = searchDatapoints_result.get('data')
            first_timestamp = chunk_datapoints[0].get('timestamp')
            last_timestamp = chunk_datapoints[-1].get('timestamp')
            datapoints_added = 0
            for datapoint in chunk_datapoints:
                data_id = datapoint.get('data_id')
                if data_id not in data_ids:
                    data_ids.add(data_id)
                    datapoints.append(datapoint)
                    datapoints_added +=1
            logger.info('Chunk {}: fetched {} results from {} to {} containing {} new datapoints'.format(
                chunk_counter,
                count,
                first_timestamp,
                last_timestamp,
                datapoints_added
            ))
            chunk_counter += 1
        return datapoints

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

    def _combined_query_expression(
        self,
        assignment_ids,
        start_time=None,
        end_time=None
    ):
        combined_query_expression_list = []
        combined_query_expression_list.append(self._assignment_ids_query_expression(assignment_ids))
        if start_time is not None:
            combined_query_expression_list.append(self._start_time_query_expression(start_time))
        if end_time is not None:
            combined_query_expression_list.append(self._end_time_query_expression(end_time))
        combined_query_expression = self._query_expression(
            operator='AND',
            children_query_expression_list=combined_query_expression_list
        )
        return combined_query_expression

    def _assignment_ids_query_expression(self, assignment_ids):
        assignment_ids_query_expression_list = []
        for assignment_id in assignment_ids:
            assigment_id_query_expression = self._query_expression(
                field='source',
                operator='EQ',
                value=assignment_id
            )
            assignment_ids_query_expression_list.append(assigment_id_query_expression)
        assignment_ids_query_expression = self._query_expression(
            operator='OR',
            children_query_expression_list=assignment_ids_query_expression_list
        )
        return assignment_ids_query_expression

    def _start_time_query_expression(self, start_time):
        start_time_honeycomb_string = self._datetime_honeycomb_string(start_time)
        start_time_query_expression = self._query_expression(
            field='timestamp',
            operator='GTE',
            value=start_time_honeycomb_string
        )
        return start_time_query_expression

    def _end_time_query_expression(self, end_time):
        end_time_honeycomb_string = self._datetime_honeycomb_string(end_time)
        end_time_query_expression = self._query_expression(
            field='timestamp',
            operator='LTE',
            value=end_time_honeycomb_string
        )
        return end_time_query_expression

    def _query_expression(
        self,
        field=None,
        operator=None,
        value=None,
        children_query_expression_list=None
    ):
        query_expression = dict()
        if field is not None:
            query_expression['field'] = field
        if operator is not None:
            query_expression['operator'] = operator
        if value is not None:
            query_expression['value'] = value
        if children_query_expression_list is not None:
            query_expression['children'] = children_query_expression_list
        return query_expression

    def _fetch_datapoints_arguments(
        self,
        query_expression,
        cursor = None
    ):
        if cursor is not None:
            arguments = {
                'query': {
                    'type': 'QueryExpression!',
                    'value': query_expression
                },
                'page': {
                    'type': 'PaginationInput',
                    'value': {
                        'cursor': cursor,
                        'max': self.read_chunk_size,
                        'sort': [
                            {
                                'direction': 'ASC',
                                'field': 'timestamp'
                            },
                            {
                                'direction': 'ASC',
                                'field': 'data_id'
                            },
                        ]
                    }
                }
            }
        else:
            arguments = {
                'query': {
                    'type': 'QueryExpression!',
                    'value': query_expression
                },
                'page': {
                    'type': 'PaginationInput',
                    'value': {
                        'max': self.read_chunk_size,
                        'sort': [
                            {
                                'direction': 'ASC',
                                'field': 'timestamp'
                            },
                            {
                                'direction': 'ASC',
                                'field': 'data_id'
                            },
                        ]
                    }
                }
            }
        return arguments

    def _delete_datapoints(self, data_ids):
        statuses = [self._delete_datapoint(data_id) for data_id in data_ids]
        return statuses

    def _delete_datapoint(self, data_id):
        deleteDatapoint_results = self.honeycomb_client.request(
            request_type='mutation',
            request_name='deleteDatapoint',
            arguments={
                'data_id': {
                    'type': 'ID',
                    'value': data_id
                }
            },
            return_object = [
                'status'
            ]
        )
        try:
            status = deleteDatapoint_results.get('status')
        except:
            raise ValueError('Received unexpected response from Honeycomb')
        return status

    def _datetime_honeycomb_string(self, timestamp):
        datetime_utc = self._python_datetime_utc(timestamp)
        datetime_honeycomb_string = datetime_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return datetime_honeycomb_string

FETCH_DATA_RETURN_OBJECT = [
    {'data': [
        'data_id',
        'timestamp',
        {'source': [
            {'... on Assignment': [
                {'environment': [
                    'name'
                ]},
                {'assigned': [
                    {'... on Device': [
                        'part_number',
                        'tag_id'
                    ]},
                    {'... on Person': [
                        'name'
                    ]}
                ]}
            ]}
        ]},
        {'file': [
            'data',
            'name',
            'contentType'
        ]}
    ]},
    {'page_info': [
        'count',
        'cursor'
    ]}
]
