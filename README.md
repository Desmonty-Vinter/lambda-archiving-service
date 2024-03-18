# Lambda Archiving Service

This repository contains the code for the Lambda Archiving Service.
This service is deployed as a Docker image to AWS Lambda.

## Prerequisites

- AWS CLI installed and configured
- Docker installed

## Updating the Code

1. Clone this repository to your local machine.
2. Make the necessary changes to the code.
3. Build the Docker image with your changes.

## Building the Docker Image

Use the following command to build the Docker image. Replace `<image-name>` with the name of your Docker image.

```bash
docker build -t <image-name> .
```

## Authenticating to AWS ECR

Before you can push your Docker image to AWS ECR, you need to authenticate your Docker client to your ECR registry. Use the following command to do this. Replace <region> and <account-id> with your AWS region and account ID.

```bash
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account-id>.dkr.ecr.<region>.amazonaws.com
```

## Tagging the Docker Image

After building the Docker image, you need to tag it so you can push it to your ECR repository. Replace <image-name>, <account-id>, <region>, and <repository-name> with your Docker image name, AWS account ID, AWS region, and ECR repository name.

```bash
docker tag <image-name>:latest <account-id>.dkr.ecr.<region>.amazonaws.com/<repository-name>:latest
```

## Pushing the Docker Image to AWS ECR

Finally, you can push your Docker image to your ECR repository. Replace <account-id>, <region>, and <repository-name> with your AWS account ID, AWS region, and ECR repository name.

```bash
docker push <account-id>.dkr.ecr.<region>.amazonaws.com/<repository-name>:latest
```

After pushing the Docker image, your updated code will be available in AWS Lambda.

## Update the lamdba function

1. Go to the AWS Lambda console.
2. Select the Lambda function you want to update.
3. Click on the "Code" tab.
4. Click on the "Deploy new image" button.
5. Select the updated Docker image from your ECR repository.
6. Click on the "Deploy" button to update the Lambda function with the new code.
7. Test the Lambda function to make sure it is working as expected.
