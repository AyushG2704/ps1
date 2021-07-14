"""
This file create an airbyte source if not exists, check its connection and finally create an airbyte connection
 with cliff destination.
"""
#mongo mongodb://ps:vCG9gNRZNoBKM@10.0.1.54:27017/admin
import sys
import logging
from datetime import datetime, timedelta
import pymongo
import requests
from bson import ObjectId


# configurations
ENV = "DEVELOPMENT"
AIRBYTE_CLIFF_DESTINATION = "Snowflake"
# mongodb configurations - docker hosted mongodb creds
MONGODB_URL = "mongodb+srv://narsi:narsi@cluster0.3pif8.mongodb.net/test?retryWrites=true&w=majority"
MONGO_DB_NAME = "test"
MONGO_STREAM_SOURCE_SUBSCRIPTION_COLLECTION = "test"

# airbyte configurations - docker hosted airbyte creds
AIRBYTE_HOST = "localhost:8000"
AIRBYTE_WORKSPACE_ID = "5ae6b09b-fdec-41af-aaf7-7d94cfc33ef6"

# Postgres configurations - docker hosted postgres creds
POSTGRES_HOST = COCKROACHDB_HOST = "0.0.0.0"                 
POSTGRES_PORT = COCKROACHDB_PORT = "8080"
POSTGRES_USER = COCKROACHDB_USER = "postgres"
POSTGRES_PASSWORD = COCKROACHDB_PASSWORD = "mysecretpassword"
POSTGRES_DATABASE = COCKROACHDB_DATABASE = "postgres"
POSTGRES_SCHEMA = COCKROACHDB_SCHEMA = "public"

class MongoDBUtils:
    """
    input - mongodb connection url
    """

    def __init__(self, connection_string):
        self.connection_string = connection_string

    def get_client(self):
        client = pymongo.MongoClient(self.connection_string)
        return client

    def get_stream_obj(self, stream_id):
        pass

    def get_subscription_obj(self, subscription_id):
        try:
            client = self.get_client()
            subscription_doc = list(
                client[MONGO_DB_NAME][MONGO_STREAM_SOURCE_SUBSCRIPTION_COLLECTION].find(
                    {"_id": ObjectId(str(subscription_id))}))
            if subscription_doc:
                subscription_doc = subscription_doc[0]
            else:
                subscription_doc = {}
            client.close()
        except Exception as e:
            logging.exception(f"Error getting subscription for subscription_id: {subscription_id}\n Error: {e}")
            subscription_doc = {}
        return subscription_doc

    def get_source_obj(self, source_id):
        pass


class SourceConnectionConfiguration:
    def __init__(self, subscription):
        self.subscription = subscription

    def get_google_sheets_config(self):
        return {}

    def get_hubspot_config(self):
        start_date = self.subscription['sync_parameters']['sync_since'] \
            if 'sync_parameters' in self.subscription and 'sync_since' in self.subscription['sync_parameters'] \
            else datetime.now() - timedelta(days=30)
        return {
            "start_date": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "credentials": {
                "api_key": self.subscription['auth_credentials']['api_key']
            }
        }

    def get_shopify_config(self):
        return {
            "start_date": str(self.subscription['sync_parameters']['sync_since'].date()) if 'sync_parameters' in self.subscription and 'sync_since' in self.subscription['sync_parameters'] else str(datetime.now() - timedelta(days=30)),
            "api_password": self.subscription['auth_credentials']['api_key'],
            "shop": self.subscription['auth_credentials']['shop_name']
        }


class CliffAirbyte:

    def __init__(self, subscription_id):   #The init method is similar to constructors in java. 
        """

        :param subscription_id: str
        subscription_id from mongodb subscription collection
        """
        self.subscription_id = subscription_id
        self.mongo_utils = MongoDBUtils(MONGODB_URL)
        self.subscription = self.mongo_utils.get_subscription_obj(self.subscription_id)
        if not self.subscription:
            sys.exit(f"No subscription found for subscription_id: {self.subscription_id}")

        self.source_config_obj = SourceConnectionConfiguration(self.subscription)
        self.source_mapper = {"Hubspot": {"name": "Hubspot", "config": self.source_config_obj.get_hubspot_config},
                              "Shopify": {"name": "Shopify", "config": self.source_config_obj.get_shopify_config},
                              "Google Sheets": {"name": "Google Sheets",
                                                "config": self.source_config_obj.get_google_sheets_config}}
        self.base_url = f"http://{AIRBYTE_HOST}"

    def check_source_connection(self, source_definition_id, connection_configuration):
        """
        this function check connection for source with given source_definition_id and connection_configuration
        :param source_definition_id: str
        airbyte source id
        :param connection_configuration: dict
        connection configuration dict
        :return: dict

        """
        body = {"sourceDefinitionId": source_definition_id, "connectionConfiguration": connection_configuration}
        url = f"{self.base_url}/api/v1/scheduler/sources/check_connection"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()
        except Exception as e:
            logging.exception(
                f"Error checking source connection for subscription_id: {self.subscription_id}\nError {e}")
            response_json = {}
        return response_json

    def check_destination_connection(self, destination_definition_id, connection_configuration):
        """
        this function check connection for destination with given source_definition_id and connection_configuration
        :param destination_definition_id: str
        airbyte destination id
        :param connection_configuration: dict
        connection configuration dict
        :return:
        """
        body = {"destinationDefinitionId": destination_definition_id,
                "connectionConfiguration": connection_configuration}
        url = f"{self.base_url}/api/v1/scheduler/destinations/check_connection"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()
        except Exception as e:
            logging.exception(
                f"Error checking destination connection for subscription_id: {self.subscription_id}\nError {e}")
            response_json = {}
        return response_json

    def get_available_sources(self):
        """
        this function returns a list of available airbyte sources
        :return: list
        """
        body = {"workspaceId": AIRBYTE_WORKSPACE_ID}
        url = f"{self.base_url}/api/v1/source_definitions/list"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['sourceDefinitions']
        except Exception as e:
            logging.exception(f"Error getting sources for subscription_id: {self.subscription_id}\nError {e}")
            response_json = []
        return response_json

    def get_available_destinations(self):
        """
        this function returns a list of available airbyte sources
        :return: list
        """
        body = {"workspaceId": AIRBYTE_WORKSPACE_ID}
        url = f"{self.base_url}/api/v1/destination_definitions/list"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['destinationDefinitions']
        except Exception as e:
            logging.exception(f"Error getting destinations for subscription_id: {self.subscription_id}\nError {e}")
            response_json = []
        return response_json

    def get_connected_sources(self):
        """
        this function returns a list of connected airbyte sources
        :return: list
        """
        body = {"workspaceId": AIRBYTE_WORKSPACE_ID}
        url = f"{self.base_url}/api/v1/sources/list"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['sources']
        except Exception as e:
            logging.exception(f"Error getting sources for subscription_id: {self.subscription_id}\nError {e}")
            response_json = []
        return response_json

    def get_connected_destinations(self):
        """
        this function returns list of connected airbyte destinations
        :return: list
        """
        body = {"workspaceId": AIRBYTE_WORKSPACE_ID}
        url = f"{self.base_url}/api/v1/destinations/list"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['destinations']
        except Exception as e:
            logging.exception(
                f"Error getting destinations for subscription_id: {self.subscription_id}\nError {e}")
            response_json = []
        return response_json

    def get_connections(self):
        """
        this function returns a list of connected airbyte connections
        :return: list
        """
        body = {"workspaceId": AIRBYTE_WORKSPACE_ID}
        url = f"{self.base_url}/api/v1/web_backend/connections/list"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['connections'] if 'connections' in response.json() else {}
        except Exception as e:
            logging.exception(f"Error getting connections for subscription_id: {self.subscription_id}\nError {e}")
            response_json = {}
        return response_json

    def create_source(self, source_definition_id, connection_configuration):
        """
        this function creates an airbyte source with given source id and configs
        :param source_definition_id: str
        airbyte source id
        :param connection_configuration: dict
        source configurations
        :return: dict
        """
        body = {"name": f"source_{self.subscription_id}",
                "workspaceId": AIRBYTE_WORKSPACE_ID,
                "sourceDefinitionId": source_definition_id,
                "connectionConfiguration": connection_configuration}
        url = f"{self.base_url}/api/v1/sources/create"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['sourceId']
        except Exception as e:
            logging.exception(
                f"Error creating source connection for subscription_id: {self.subscription_id}\nError {e}")
            response_json = ""
        return response_json

    def create_destination(self, destination_definition_id, connection_configuration):
        """
        this function creates an airbyte destination with given destination id and configs
        :param destination_definition_id: str
        airbyte destination id
        :param connection_configuration: dict
        source configurations
        :return: dict
        """
        body = {"name": f"CLIFF_{ENV}",
                "workspaceId": AIRBYTE_WORKSPACE_ID,
                "destinationDefinitionId": destination_definition_id,
                "connectionConfiguration": connection_configuration}
        url = f"{self.base_url}/api/v1/destinations/create"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['destinationId']
        except Exception as e:
            logging.exception(
                f"Error creating destination for subscription_id: {self.subscription_id}\nError {e}")
            response_json = ""
        return response_json

    def get_discover_schema(self, source_id):
        """
        this function return the available schema for the given source id
        :param source_id: str
        airbyte source_id
        :return: dict
        """
        body = {"sourceId": source_id}
        url = f"{self.base_url}/api/v1/sources/discover_schema"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()['catalog']
        except Exception as e:
            logging.exception(f"Error getting schema for subscription_id: {self.subscription_id}\nError {e}")
            response_json = {}
        return response_json

    def create_connection(self, source_id, destination_id, sync_catalog, schedule, prefix="", status="active",  normalization=False):
        """
        This function creates an airbyte connection with the given params
        :param source_id: str
        airbyte source id
        :param destination_id: str
        airbyte destination id
        :param sync_catalog: dict
        source catalog to sync
        :param schedule: dict
        schedule params
        :param prefix: str
        prefix to add to tables
        :param status: str
        intial status
        :return: dict
        """
        body = {"name": f"connection_{self.subscription_id}", "sourceId": source_id, "destinationId": destination_id,
                "schedule": schedule, "prefix": prefix, "status": status, "syncCatalog": sync_catalog,
                "namespaceDefinition": "destination", "namespaceFormat": "${SOURCE_NAMESPACE}",
                }

        if normalization:
            body.update({"operations": [
                {
                    "name": "Normalization",
                    "operatorConfiguration": {
                        "operatorType": "normalization",
                        "normalization": {
                            "option": "basic"
                        }
                    }
                }
            ]})
        url = f"{self.base_url}/api/v1/web_backend/connections/create"
        response = requests.post(url, json=body)
        try:
            response_json = response.json()
        except Exception as e:
            logging.exception(
                f"Error creating airbyte source-destination connection for subscription_id: {self.subscription_id}\nError {e}")
            response_json = {}
        return response_json

    def delete_connection(self, connection_id):
        """
        this function deletes airbyte source-destination connection for the given connection_id
        :param connection_id: str
        :return:
        """
        body = {"connectionId": connection_id}
        url = f"{self.base_url}/api/v1/connections/delete"
        response = requests.post(url, json=body)
        return response.status_code

    def delete_source(self, source_id):
        """
        this function deletes airbyte source for the given source_id
        :param source_id: str
        :return:
        """
        body = {"sourceId": source_id}
        url = f"{self.base_url}/api/v1/sources/delete"
        response = requests.post(url, json=body)
        return response.status_code

    def delete_destination(self, destination_id):
        """
        this function deletes airbyte destination for the given destination_id
        :param destination_id: str
        :return:
        """
        body = {"destinationId": destination_id}
        url = f"{self.base_url}/api/v1/destinations/delete"
        response = requests.post(url, json=body)
        return response.status_code

    @staticmethod
    def get_destination_configuration():      
        destination_configuration = { "role":"SYSADMIN",
                                     "username": "NARASIMHA", "schema": "public",
                                     "database": "NEW2","warehouse":"COMPUTE_WH" ,"password":"p59Qwgi8GN2sGTU",
                                     "host": "hba18666.us-east-1.snowflakecomputing.com"}
        return destination_configuration

    def get_sync_catalog(self, source_id):
        stream_catalog = self.get_discover_schema(source_id)
        for stream in stream_catalog['streams']:
            if 'sync_parameters' in self.subscription and 'collections' in self.subscription['sync_parameters'] and \
                    self.subscription['sync_parameters']['collections']:
                collection = [collection for collection in self.subscription['sync_parameters']['collections'] if
                              collection['collection_name'] == stream['stream']['name']]
                stream['config']['selected'] = True if collection and 'user_selection' in collection[0] and \
                                                       collection[0]['user_selection'] else False
            else:
                stream['config']['selected'] = True
            # stream['config']['syncMode'] = self.subscription['sync_parameters'][
            #     'replication_method'] if 'sync_parameters' in self.subscription and 'replication_method' in \
            #                              self.subscription['sync_parameters'] else "full_refresh"
        return stream_catalog

    def get_schedule(self):
        frequency = int(self.subscription['sync_frequency']) if 'sync_frequency' in self.subscription else 3600
        if frequency in [60 * 5, 60 * 15, 60 * 30]:
            units = frequency / 60
            time_unit = "minutes"
        elif frequency in [60 * 60 * 1, 60 * 60 * 3, 60 * 60 * 3, 60 * 60 * 6, 60 * 60 * 8, 60 * 60 * 12, 60 * 60 * 24]:
            units = frequency / (60 * 60)
            time_unit = "hours"
        else:
            units = 1
            time_unit = "hours"

        schedule = {
            "units": units,
            "timeUnit": time_unit
        }
        return schedule

    def main(self):
        """
        this function does everything
        step0: check if airbyte connection exists with given subscription (if yes - exit, else continue)
        step1: get subscription object
        step2: get source name
        step3: get source credentials
        step4: check source connection
        step5: create source
        step6: check for destination connection
        step7: create airbyte connection
        :return:
        """
        # step0:
        airbyte_connections = [source['name'] for source in self.get_connections()]
        if f"connection_{self.subscription_id}" in airbyte_connections:
            logging.info(
                f"Airbyte connection already exists with the given subscription_id: {self.subscription_id}")
        else:
            source_id = [row['sourceId'] for row in self.get_connected_sources() if
                         row['name'] == f"source_{self.subscription_id}"]
            if source_id:
                source_id = source_id[0]
            else:
                source_name = self.subscription['source']['name']
                source_definition_id = [row['sourceDefinitionId'] for row in self.get_available_sources() if
                                        row['name'] == self.source_mapper[source_name]['name']]
                if source_definition_id:
                    source_definition_id = source_definition_id[0]
                    source_configuration = self.source_mapper[source_name]['config']()
                    source_id = self.create_source(source_definition_id, source_configuration)
                else:
                    source_id = None

            destination_id = [row['destinationId'] for row in self.get_connected_destinations() if
                              row['name'] == f"CLIFF_{ENV}"]
            if destination_id:
                destination_id = destination_id[0]
            else:
                destination_definition_id = [row['destinationDefinitionId'] for row in self.get_available_destinations()
                                             if row['name'] == AIRBYTE_CLIFF_DESTINATION]
                if destination_definition_id:
                    destination_configuration = self.get_destination_configuration()
                    destination_id = destination_id if destination_id else self.create_destination(
                        destination_definition_id[0], destination_configuration)
                else:
                    destination_id = None
            if destination_id:
                if source_id:
                    sync_catalog = self.get_sync_catalog(source_id)
                    schedule = self.get_schedule()
                    self.create_connection(source_id, destination_id, sync_catalog, schedule,
                                           prefix=f"sync_table_{self.subscription_id}_", status="active",
                                           normalization=False)
                                          
                                          
                else:
                    logging.warning(f"Cannot find source_id for subscription {self.subscription_id}")
            else:
                logging.warning(f"Cannot find destination_id for subscription {self.subscription_id}")


if __name__ == "__main__":
    cliff_airbyte_obj = CliffAirbyte(sys.argv[1])
    cliff_airbyte_obj.main()
