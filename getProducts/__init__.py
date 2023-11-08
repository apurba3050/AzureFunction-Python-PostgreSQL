import logging

import azure.functions as func
import psycopg2
import psycopg2.pool
import json


# Connection details
host = "localhost"
user = "postgres"
password = "Khulna198#"
db_name = "apurba_data"

# Create a connection pool
pool = psycopg2.pool.SimpleConnectionPool(
    1, # minimum number of connections
    20, # maximum number of connections
    host=host,
    user=user,
    password=password,
    dbname=db_name
)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        query = "SELECT * FROM products"
        # Get a connection from the pool
        connection = pool.getconn()
        
        # Create a cursor
        cursor = connection.cursor()
        logging.info('Max connection.' + str (pool.maxconn))
        # Execute the query
        cursor.execute(query)
        # Fetch the results
        columns = list(cursor.description)
        result = cursor.fetchall()
        # Close the cursor and connection
        cursor.close()
        pool.putconn(connection)
        #print(rows)
        results = []
        for row in result:
            row_dict ={}
            for i, col in enumerate(columns):
                row_dict[col.name] = row[i]
            results.append(row_dict)
        #print(results)
        results_json = json.dumps(results)
    except (Exception, psycopg2.Error) as error:
        print("Error while running the query", error)

   
    return func.HttpResponse(          
            results_json
            
    )
