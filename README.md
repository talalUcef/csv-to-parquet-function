# Project

A simple AWS lambda function that converts a csv file from S3 to Parquet format and put it into S3.

The project contains source code and supporting files for a serverless application that you can deploy with the SAM CLI. It includes the following files and folders.

- csv_to_parquet_function - Code for the application's Lambda function.
- template.yaml - A template that defines the application's AWS resources.

The application uses several AWS resources, including Lambda functions and S3. These resources are defined in the `template.yaml` file in this project. You can update the template to add AWS resources through the same deployment process that updates your application code.


# Build and deploy

## Build
To build the function using [the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)

```bash
cd {project_dir}
sam build --template template.yaml --build-dir .aws-sam/build
```

## Package
To package the function, the following command will use the bucket csv-to-parquet-function-config-bucket to save the function config. 


```bash
aws s3api create-bucket --bucket csv-to-parquet-function-config-bucket \
                        --region us-east-1

sam package --template-file .aws-sam\build\template.yaml \
            --output-template-file .aws-sam\build\packaged-template.yaml \
            --s3-bucket csv-to-parquet-function-config-bucket
```

## Deploy
To deploy to AWS, the following command will create a CloudFormation stack with the configured resources in the `template.yaml` file.

```bash
sam deploy  --template-file .aws-sam\build\packaged-template.yaml \
            --stack-name csv-to-parquet-function-stack \
            --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
            --region us-east-1
```

## Cleanup

To delete the sample application that you created, use the AWS CLI. Assuming you used your project name for the stack name, you can run the following:

```bash
aws s3 rm s3://titanic-datalake --recursive
aws s3 rm s3://csv-to-parquet-function-config-bucket --recursive
aws cloudformation delete-stack --stack-name csv-to-parquet-function-stack \
                                --region us-east-1
```