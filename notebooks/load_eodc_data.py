import openeo
backend = "https://openeo.eodc.eu"

connection = openeo.connect(backend)
connection.authenticate_oidc(provider_id='egi')
connection.list_collection_ids()
connection.list_collections()
connection.list_processes()
