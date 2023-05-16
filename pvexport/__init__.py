import logging
import pvutils
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    purview_account = req.params.get('purview_account')
    keywords = req.params.get('keywords')
    asset_type = req.params.get('asset_type')
    storage_account_url = req.params.get('storage_account_url')

    # Verify that the required parameters are provided
    if not purview_account:
        logging.error('purview_account parameter is required')
        return func.HttpResponse(
            'purview_account parameter is required',
            status_code=400
        )
    if not storage_account_url:
        logging.error('storage_account_url parameter is required')
        return func.HttpResponse(
            'storage_account_url parameter is required',
            status_code=400
        )
    
    # if no keywords are provided, use '*' to return all assets
    keywords = keywords if keywords else '*'

    try:
        filter = pvutils.create_filter(asset_type)
        purview_client = pvutils.purview_client(purview_account)
        pv_search_df = pvutils.query_to_dataframe(purview_client, keywords, filter)
        filename = pvutils.export_to_csv(purview_client, pv_search_df, storage_account_url)
        logging.info(f'File exported successfully to {storage_account_url}{filename}')
    except Exception as e:
        logging.error(e)
        return func.HttpResponse(
            'An internal error occurred',
            status_code=500
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
        )
