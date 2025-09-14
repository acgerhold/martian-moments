import pandas as pd

def generate_ingestion_batches_from_table_results(table_results, logger):
    logger.info("Extracting ingestion schedule from table results")
    table_results_dataframe = pd.DataFrame(table_results)

    batches = []
    for _, row in table_results_dataframe.iterrows():
        if row['START_SOL'] < row['MAX_SOL']:
            batch = {
                "rover_name": row['ROVER_NAME'],
                "sol_start": row['START_SOL'],
                "sol_end": row['SOL_END']
            }
            batches.append(batch)

    logger.info("Successfully generated batches for ingestion run")
    return batches
