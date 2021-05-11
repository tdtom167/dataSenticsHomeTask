import boto3


def create_dax_table():
    """
    Creates a DynamoDB table.

    :param dyn_resource: Either a Boto3 or DAX resource.
    :return: The newly created table.
    """
    
    table_name = 'TryDaxTable'
    params = {
        'TableName': table_name,
        'Endpoint-url': 'http://localhost:8000
        'KeySchema': [
            {'AttributeName': 'partition_key', 'KeyType': 'HASH'},
            {'AttributeName': 'sort_key', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'partition_key', 'AttributeType': 'N'},
            {'AttributeName': 'sort_key', 'AttributeType': 'N'}
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    table = dyn_resource.create_table(**params)
    print(f"Creating {table_name}...")
    table.wait_until_exists()
    return table


if __name__ == '__main__':
    dax_table = create_dax_table()
    print(f"Created table.")