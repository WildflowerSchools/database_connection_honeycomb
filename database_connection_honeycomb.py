from database_connection import DatabaseConnection
from database_connection import DataQueue
import honeycomb
import os

class DatabaseConnectionHoneycomb(DatabaseConnection):
    """
    Class to define a DatabaseConnection to Wildflower's Honeycomb database
    """
    def __init__(
        self,
        environment_name_honeycomb,
        timestamp_field_name_input = None,
        timestamp_field_name_honeycomb = None,
        object_id_field_name_input = None,
        object_id_field_name_honeycomb = None,
        honeycomb_uri = None,
        honeycomb_token_uri = None,
        honeycomb_audience = None,
        honeycomb_client_id = None,
        honeycomb_client_secret = None
    ):
        """
        Constructor for DatabaseConnectionHoneycomb.

        If timestamp_field_name_input and timestamp_field_name_honeycomb are
        specified, the database connection is assumed to be handling time series
        data, every datapoint written to the database must contain a field with
        the timestamp_field_name_input name, and this will enable various
        time-related methods to access the data (e.g., fetching a time span,
        creating an iterator that returns data points in time order).

        If object_id_field_name_input and object_id_field_name_honeycomb are
        specified, the database is assumed to be handling data associated with
        objects (e.g., measurement devices), every datapoint written to the
        database must contain a field with the object_id_field_name_input name,
        and this will enable various object methods to access the data (e.g.,
        fetching all data associated with a specific list of object IDs).

        Parameters:
            environment_name_honeycomb (string): Name of the environment that the data should be associated with
            timestamp_field_name_input (string): Name of the input field containing the timestamp for each datapoint
            timestamp_field_name_honeycomb (string): Name of the Honeycomb field containing the timestamp for each datapoint
            object_id_field_name_input (string): Name of the input field containing the object ID for each datapoint
            object_id_field_name_honeycomb (string): Name of the Honeycomb field containing the object ID for each datapoint
        """
        self.environment_name_honeycomb = environment_name_honeycomb
        self.timestamp_field_name_input = timestamp_field_name_input
        self.timestamp_field_name_honeycomb = timestamp_field_name_honeycomb
        self.object_id_field_name_input = object_id_field_name_input
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
