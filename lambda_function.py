import json
import attr
import boto3
from botocore.exceptions import ClientError
#we need to provide database details
dynamodb = boto3.resource('dynamodb')
print(dynamodb)
table_name = 'EmployeeTable'
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    http_method = event['httpMethod']

    if http_method == 'POST':
        return create_employee(event)
    elif http_method == 'PUT':
        return update_employee(event)
    elif http_method == 'DELETE':
        return delete_employee(event)
    elif http_method == 'GET':
        return get_employee(event)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Invalid HTTP Method', 'success': False})
        }

def create_employee(event):
    try:
        # Parse the request body
        employee_data = json.loads(event['body'])

        # Validate required fields
        required_fields = ['name', 'email', 'age', 'gender', 'phoneNo', 'addressDetails','hno','street','city','state',
                            'workExperience','companyName','fromdate','todate','address', 'qualifications','qualificationName','percentage', 'projects', 'title','description','photo']
        for field in required_fields:
            if field not in employee_data or not employee_data[field]:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Invalid body request. Missing required fields.', 'success': False})
                }

        # Check for duplicate email
        email = employee_data['email']
        if is_email_duplicate(email):
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Employee already exists', 'success': False})
            }

        # Validate data types
        validate_data_types(employee_data)

        # Generate a unique regid (email is unique)
        regid = "EMP" + str(hash(email))

        # Add regid to employee_data
        employee_data['regid'] = regid

        # Store employee data in DynamoDB
        table.put_item(Item=employee_data)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Employee created successfully', 'regid': regid, 'success': True})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Employee creation failed', 'success': False})
        }

def update_employee(event):
    try:
        # Parse the request body
        employee_data = json.loads(event['body'])

        # Validate required fields
        # required_fields = ['regid', 'name', 'email', 'age', 'gender', 'phoneNo', 'addressDetails',
        #                     'workExperience', 'qualifications', 'projects', 'photo']
        required_fields = ['name', 'email', 'age', 'gender', 'phoneNo', 'addressDetails','hno','street','city','state',
                            'workExperience','companyName','fromdate','todate','address', 'qualifications','qualificationName','percentage', 'projects', 'title','description','photo']
        for field in required_fields:
            if field not in employee_data or not employee_data[field]:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Invalid body request. Missing required fields.', 'success': False})
                }

        # Check if employee exists
        regid = employee_data['regid']
        if not is_employee_exists(regid):
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No employee found with this regid', 'success': False})
            }

        # Validate data types
        validate_data_types(employee_data)

        # Update employee data in DynamoDB
        table.update_item(
            Key={'regid': regid},
            UpdateExpression='SET #name = :name, #email = :email, #age = :age, #gender = :gender, #phoneNo = :phoneNo, '
                             '#addressDetails = :addressDetails,#hno =:hno,#street = :street,#city = :city,state =:state, #workExperience = :workExperience,#companyName =:companyName,#fromdate =:fromdate,#todate =:todate,#address =:address '
                             '#qualifications = :qualifications,#qualificationName =:qualificationName,#percentage =:percentage, #projects = :projects, #title =:title,#description=:description,#photo = :photo',
            ExpressionAttributeNames={'#name': 'name', '#email': 'email', '#age': 'age', '#gender': 'gender',
                                      '#phoneNo': 'phoneNo', '#addressDetails': 'addressDetails',
                                      '#hno':'hno','#street':'street','#city':'city','#state':'state',
                                      '#workExperience': 'workExperience', '#qualifications': 'qualifications','#qualificationName':'qualificationName','#percentage':'percentage',
                                      '#projects': 'projects','title':'title','#description':'description', '#photo': 'photo'},
            ExpressionAttributeValues={
                ':name': employee_data['name'],
                ':email': employee_data['email'],
                ':age': employee_data['age'],
                ':gender': employee_data['gender'],
                ':phoneNo': employee_data['phoneNo'],
                ':addressDetails': employee_data['addressDetails'],
                ':hno': employee_data['hno'],
                'city': employee_data['city'],
                'street': employee_data['street'],
                ':state': employee_data['state'],
                ':workExperience': employee_data['workExperience'],
                ':qualifications': employee_data['qualifications'],
                ':qualificationName': employee_data['qualificationName'],
                ':projects': employee_data['projects'],
                ':title': employee_data['title'],
                ':description': employee_data['description'],
                ':photo': employee_data['photo'],
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Employee details updated successfully', 'success': True})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Employee updation failed', 'success': False})
        }

def delete_employee(event):
    try:
        # Parse the request body
        request_data = json.loads(event['body'])

        # Validate required fields
        if 'regid' not in request_data or not request_data['regid']:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid body request. Missing required fields.', 'success': False})
            }

        # Check if employee exists
        regid = request_data['regid']
        if not is_employee_exists(regid):
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No employee found with this regid', 'success': False})
            }

        # Delete employee from DynamoDB
        table.delete_item(
            Key={'regid': regid}
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Employee deleted successfully', 'success': True})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Employee deletion failed', 'success': False})
        }

def get_employee(event):
    try:
        # Parse query parameters
        regid = event['queryStringParameters'].get('regid', None)

        if regid:
            # Get details of a specific employee
            employee_data = get_employee_details(regid)
            if employee_data:
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'Employee details found', 'success': True, 'employees': [employee_data]})
                }
            else:
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'Employee details not found', 'success': False, 'employees': []})
                }
        else:
            # Get details of all employees
            all_employees_data = get_all_employees_details()
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Employee details found', 'success': True, 'employees': all_employees_data})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'unable to retrieving employee details', 'success': False})
        }

def is_email_duplicate(email):
    response = table.scan(
        FilterExpression=attr('email').eq(email)
    )
    return len(response['Items']) > 0

def is_employee_exists(regid):
    response = table.get_item(
        Key={'regid': regid}
    )
    return 'Item' in response

def validate_data_types(employee_data):
    
    # For simplicity, assuming all data types are valid
    pass

def get_employee_details(regid):
    response = table.get_item(
        Key={'regid': regid}
    )
    return response.get('Item', None)

def get_all_employees_details():
    response = table.scan()
    return response.get('Items', [])

# Additional helper function for DynamoDB attribute validation
def validate_attribute(attribute, expected_type):
    if not isinstance(attribute, expected_type):
        raise ValueError(f"Invalid data type for attribute. Expected {expected_type}, got {type(attribute)}")
