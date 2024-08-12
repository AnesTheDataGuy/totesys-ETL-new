
data "archive_file" "layer" { # create a deployment package for the layer.
  type             = "zip"
  output_file_mode = "0666"
  source_dir      = "${path.module}/../layer" 
  output_path      = "${path.module}/../layer.zip"
}

resource "aws_s3_object" "layer_code" { #Upload the layer code to the code_bucket.
  bucket = aws_s3_bucket.lambda_bucket.bucket
  source = "${path.module}/../layer.zip" 
  key    = "layer.zip"
}

resource "aws_lambda_layer_version" "lambda_layer" {
  layer_name = "lambda_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.lambda_bucket.bucket # or aws_s3_bucket.lambda_bucket.id ?
  s3_key              = aws_s3_object.layer_code.key
  depends_on          = [aws_s3_object.layer_code] # triggered only if the zip file is uploaded to the bucket
  #  skip_destroy      = true
}
