import boto3
from botocore.exceptions import ClientError

def send_email():
    SENDER = "gordeninottawa@gmail.com"

    # Send to myself
    RECIPIENT = "gordeninottawa@gmail.com"

    AWS_REGION = "us-east-1"

    # The subject line for the email.
    SUBJECT = "Files missing in S3 bucket"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("Files missing in AWS S3 bucket. Airflow API is not triggered. Please check Snowflake task.")

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    # Put in aws_access_key_id and aws_secret_access_key, Don't upload to github
    client = boto3.client('ses',region_name=AWS_REGION, aws_access_key_id="",aws_secret_access_key="")

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER

        )
    # Display an error if something goes wrong. 
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])