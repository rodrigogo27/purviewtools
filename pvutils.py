import datetime
import math
import pandas as pd

from io import BytesIO
from azure.identity import DefaultAzureCredential
from azure.purview.catalog import PurviewCatalogClient
from azure.storage.blob import BlobServiceClient

def split_storage_acccount_url(storage_account_url):
    #split into 4 parts: storage url, container, folder
    storage_url = storage_account_url.split('.net/')[0] + '.net'
    container = storage_account_url.split('.net/')[1].split('/')[0]
    #remove storage_url, container and beginning / from storage_account_url
    folder = storage_account_url.replace(storage_url + '/', '').replace(container, '')[1:]
    return storage_url, container, folder

def create_filter(asset_type):
    filter = {
        "and": [
            {"objectType": "Tables"},
            {"assetType": asset_type}
        ]}
    return filter

def create_search_body(keywords, filter):
    search_body = {
        'keywords': keywords if keywords else None,
        'facets': None,
        'filter': filter if filter else None,
    }
    return search_body

def purview_client(purview_account):
    credential = DefaultAzureCredential()
    client = PurviewCatalogClient(
        endpoint=f'https://{purview_account}.purview.azure.com', 
        credential=credential,
        logging_enable=True)
    return client

def storage_client(storage_url):
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(storage_url, credential=credential)
    return blob_service_client

def query_to_dataframe(purview_client, keywords, filter):
    search_request = create_search_body(keywords, filter)
    purview_search = purview_client.discovery.query(search_request=search_request)
    search_df = pd.DataFrame.from_dict(purview_search['value'])
    return search_df

def get_asset(purview_client, asset_id):
    asset = purview_client.entity.get_by_guid(asset_id)
    return asset

def get_term_guid(purview_client, term):
    term_guid = purview_client.glossary.get_term_by_name(term)['guid']
    return term_guid

def related_entities_to_dataframe(asset):
    related_entities_df = pd.DataFrame.from_dict(asset['referredEntities'])
    return related_entities_df

def export_to_csv(purview_client, search_df, storage_account_url):
    output_df = pd.DataFrame(columns=[
        'table_guid',
        'column_guid',
        'qualifiedName',
        'assetType',
        'schemaName',
        'tableName',
        'columnName',
        'columnDescription'
    ])

    for iAsset, asset in search_df.iterrows():
        pv_asset = get_asset(purview_client, asset['id'])
        related_df = pd.DataFrame.from_dict(pv_asset['referredEntities'])

        for iEntity, relatedEntity in related_df.items():
            if 'column' in relatedEntity['typeName']:
                col_df = pd.DataFrame.from_records([{
                    'table_guid': asset['id'],
                    'column_guid': relatedEntity['guid'],
                    'qualifiedName': relatedEntity['attributes']['qualifiedName'],
                    'assetType': asset['assetType'][0],
                    'schemaName': pv_asset['entity']['relationshipAttributes']['dbSchema']['displayText'] if 'dbSchema' in pv_asset['entity']['relationshipAttributes'] else None,
                    'tableName': asset['name'],
                    'columnName': relatedEntity['attributes']['name'],
                    'columnDescription': relatedEntity['attributes']['userDescription']
                }])
                output_df = pd.concat([output_df, col_df], ignore_index=True)

    output_csv = output_df.to_csv(index=False)
    fileTS = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'purview_assets_{fileTS}.csv'
    storage_url, container, folder = split_storage_acccount_url(storage_account_url)
    filepath = folder + '/' + filename
    storage_account = storage_client(storage_url)
    blob_client = storage_account.get_blob_client(container=container, blob=filepath)
    blob_client.upload_blob(output_csv, overwrite=True)

    return filename

def load_blob_to_dataframe(storage_blob):
    storage_url, container, blob = split_storage_acccount_url(storage_blob)
    storage_account = storage_client(storage_url)
    blob_client = storage_account.get_blob_client(container=container, blob=blob)
    #blob_stream = blob_client.download_blob().readall()
    with BytesIO() as input_blob:
        blob_client.download_blob().download_to_stream(input_blob)
        input_blob.seek(0)
        blob_df = pd.read_csv(input_blob)
    
    return blob_df

def transpose_mappings(purview_client, mappings_df):
    transposed_output = {
        "glossaryTerm": str,
        "term_guid": str,
        "columns": object
    }
    transposed_df = pd.DataFrame(columns=transposed_output.keys()).astype(transposed_output)

    #read all rows from the glossaryTerms column
    glossaryTerms = mappings_df['glossaryTerms'].tolist()
    #split glossaryTerms in each row by comma

    #remove all nan from list
    glossaryTerms = [x for x in glossaryTerms if str(x) != 'nan']

    # if term has commas, split into multiple rows
    for term in glossaryTerms:
        if ',' in term:
            #get index of term
            i = glossaryTerms.index(term)
            #split term into multiple rows
            term_list = term.split(',')
            #remove term from glossaryTerms
            glossaryTerms.remove(term)
            #insert term_list into glossaryTerms
            glossaryTerms[i:i] = term_list
    
    #remove duplicates from flossaryTerms
    glossaryTerms = list(dict.fromkeys(glossaryTerms))

    #add a comma at the end of each value in glossaryTerms column in mappings_df
    mappings_df['glossaryTerms'] = mappings_df['glossaryTerms'].astype(str) + ','

    #get term guid for each term
    for term in glossaryTerms:
        term_guid = get_term_guid(purview_client, term)
        #find all column_guid in mappings_df that contain the term within the glossaryTerms column
        term_df = mappings_df[mappings_df['glossaryTerms'].str.contains(term + ',')]
        columns = term_df['column_guid'].tolist()
        row_df = pd.DataFrame.from_records([{
            'glossaryTerm': term,
            'term_guid': term_guid,
            'columns': columns
        }])
        transposed_df = pd.concat([transposed_df, row_df], ignore_index=True)
        
    return transposed_df

def create_mappings(purview_client, mappings_df):
    return