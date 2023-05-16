import logging
import pvutils
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    purview_account = req.params.get('purview_account')
    storage_blob = req.params.get('storage_blob')

    # Verify that the required parameters are provided
    if not purview_account:
        logging.error('purview_account parameter is required')
        return func.HttpResponse(
            'purview_account parameter is required',
            status_code=400
        )
    if not storage_blob:
        logging.error('storage_blob parameter is required')
        return func.HttpResponse(
            'storage_blob parameter is required',
            status_code=400
        )
    
    try:
        purview_client = pvutils.purview_client(purview_account)
        #load blob to dataframe
        mappings_df = pvutils.load_blob_to_dataframe(storage_blob)
        mappings_transposed_df = pvutils.transpose_mappings(purview_client, mappings_df)
        pvutils.create_mappings(purview_client, mappings_transposed_df)
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
