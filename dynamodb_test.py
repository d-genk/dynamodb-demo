import boto3

def create_dynamodb_table(table_name):
    # Create a boto3 client for DynamoDB
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')

    # Define the table schema
    table_schema = {
        'TableName': table_name,
        'KeySchema': [
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'id',
                'AttributeType': 'S'  # S = String, N = Number, B = Binary
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',  # Or 'PROVISIONED' for provisioned throughput settings
    }

    # Create the table
    try:
        table = dynamodb.create_table(**table_schema)
        print(f"Table {table_name} is being created.")
        return table
    except Exception as e:
        print(f"Error creating table: {e}")
        return None

"""# Example usage
if __name__ == "__main__":
    table_name = 'MySampleTable'
    create_dynamodb_table(table_name)
"""

def add_item_to_table(table_name, item):
    # Initialize a boto3 DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1') # Adjust region if needed

    # Select your DynamoDB table
    table = dynamodb.Table(table_name)

    # Add the item to the table
    try:
        response = table.put_item(Item=item)
        print("Item added successfully:", response)
    except Exception as e:
        print("Error adding item to table:", e)

"""# Example usage
if __name__ == "__main__":
    table_name = 'MySampleTable'
    item = {
        'id': '1',  # Assuming 'id' is the primary key
        'name': 'John Doe',
        'age': 30,
        'email': 'johndoe@example.com'
    }
    add_item_to_table(table_name, item)
"""

def get_item_from_table(table_name, key):
    # Initialize a boto3 DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1') # Adjust the region as necessary

    # Select the DynamoDB table
    table = dynamodb.Table(table_name)

    # Retrieve the item from the table
    try:
        response = table.get_item(Key=key)
        item = response.get('Item', None)
        if item:
            print("Retrieved item:", item)
            return item
        else:
            print("Item not found.")
            return None
    except Exception as e:
        print("Error retrieving item from table:", e)
        return None

"""# Example usage
if __name__ == "__main__":
    table_name = 'MySampleTable'
    key = {
        'id': '1',  # Assuming 'id' is the primary key
    }
    get_item_from_table(table_name, key)
"""

def query_items(table_name, partition_key_value):
    # Initialize a boto3 DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    # Select the DynamoDB table
    table = dynamodb.Table(table_name)

    # Perform the query operation on the table
    try:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('id').eq(partition_key_value)
        )
        items = response.get('Items', [])
        print(f"Query returned {len(items)} items.")
        return items
    except Exception as e:
        print(f"Error querying table: {e}")
        return []

"""# Example usage
if __name__ == "__main__":
    table_name = 'YourTableName'
    partition_key_value = 'YourPartitionKeyValue'
    query_items(table_name, partition_key_value)"""

def scan_items(table_name, filter_expression):
    # Initialize a boto3 DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    # Select the DynamoDB table
    table = dynamodb.Table(table_name)

    # Perform the scan operation on the table
    try:
        response = table.scan(
            FilterExpression=filter_expression
        )
        items = response.get('Items', [])
        print(f"Scan returned {len(items)} items.")
        return items
    except Exception as e:
        print(f"Error scanning table: {e}")
        return []

"""# Example usage
if __name__ == "__main__":
    table_name = 'YourTableName'
    # Example filter: age greater than 25
    filter_expression = boto3.dynamodb.conditions.Attr('age').gt(25)
    scan_items(table_name, filter_expression)"""

def query_items_with_projection(table_name, partition_key_value, projection):
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('id').eq(partition_key_value),
            ProjectionExpression=projection  # Adding projection expression here
        )
        items = response.get('Items', [])
        print(f"Query with projection returned {len(items)} items.")
        return items
    except Exception as e:
        print(f"Error querying table with projection: {e}")
        return []

"""# Example usage
if __name__ == "__main__":
    table_name = 'YourTableName'
    partition_key_value = 'YourPartitionKeyValue'
    projection = "name, email"  # Specify the attributes you want to retrieve
    query_items_with_projection(table_name, partition_key_value, projection)"""

def scan_items_with_projection(table_name, filter_expression, projection):
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table(table_name)

    try:
        response = table.scan(
            FilterExpression=filter_expression,
            ProjectionExpression=projection  # Adding projection expression here
        )
        items = response.get('Items', [])
        print(f"Scan with projection returned {len(items)} items.")
        return items
    except Exception as e:
        print(f"Error scanning table with projection: {e}")
        return []

"""# Example usage
if __name__ == "__main__":
    table_name = 'YourTableName'
    filter_expression = boto3.dynamodb.conditions.Attr('age').gt(25)
    projection = "name, email"  # Specify the attributes you want to retrieve
    scan_items_with_projection(table_name, filter_expression, projection)"""



