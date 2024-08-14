data "archive_file" "test_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/lambda_functions/test_lambda.py"
  output_path      = "${path.module}/../zip_code/test_lambda.zip"
}

data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/lambda_functions/extract.py"
  output_path      = "${path.module}/../zip_code/extract.zip"
}

data "archive_file" "load_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/lambda_functions/load.py"
  output_path      = "${path.module}/../zip_code/load.zip"
}

data "archive_file" "transform_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/lambda_functions/transform.py"
  output_path      = "${path.module}/../zip_code/transform.zip"
}

resource "aws_s3_object" "test_lambda_zip" { #Upload the lambda zip to lambda_bucket.
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/test_lambda.zip"
  key    = "test_lambda.zip"
}

resource "aws_s3_object" "extract_lambda_zip" {
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/extract.zip"
  key    = "extract.zip"
}

resource "aws_s3_object" "load_lambda_zip" {
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/load.zip"
  key    = "load.zip"
}

resource "aws_s3_object" "transform_lambda_zip" {
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../zip_code/transform.zip"
  key    = "transform.zip"
}

resource "aws_lambda_function" "test_lambda" { #Provision the lambda
  s3_bucket        = aws_s3_bucket.lambda_bucket.id
  s3_key           = aws_s3_object.test_lambda_zip.key
  function_name    = "test_lambda"
  source_code_hash = data.archive_file.test_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.lambda_layer.arn]
  runtime          = var.python_runtime
  handler          = "test_lambda.lambda_handler"
}

resource "aws_lambda_function" "extract_lambda" { #Provision the lambda
  s3_bucket        = aws_s3_bucket.lambda_bucket.id
  s3_key           = aws_s3_object.extract_lambda_zip.key
  function_name    = "extract"
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.lambda_layer.arn]
  runtime          = var.python_runtime
  handler          = "extract.lambda_handler"
}

resource "aws_lambda_function" "load_lambda" { #Provision the lambda
  s3_bucket        = aws_s3_bucket.lambda_bucket.id
  s3_key           = aws_s3_object.load_lambda_zip.key
  function_name    = "load"
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.lambda_layer.arn]
  runtime          = var.python_runtime
  handler          = "load.lambda_handler"
}

resource "aws_lambda_function" "transform_lambda" { #Provision the lambda
  s3_bucket        = aws_s3_bucket.lambda_bucket.id
  s3_key           = aws_s3_object.transform_lambda_zip.key
  function_name    = "transform"
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  layers           = [aws_lambda_layer_version.lambda_layer.arn]
  runtime          = var.python_runtime
  handler          = "transform.lambda_handler"
}
