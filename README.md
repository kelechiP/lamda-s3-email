# S3 → Lambda → SNS Email Automation (Ansible + Jenkins)

This project deploys an automated pipeline where:
- Uploading a `.txt` file to S3
- Triggers a Lambda function
- Sends the file content via SNS email

## Tech Stack
- AWS Lambda
- Amazon S3
- Amazon SNS
- Ansible (boto3)
- Jenkins

## Deployment
Triggered via Jenkins pipeline with environment selection.

## Prerequisites
- AWS credentials or IAM role
- Python 3.9+
- Ansible
- boto3

## Usage
Upload a `.txt` file to the configured S3 bucket.
