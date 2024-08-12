
#define variables
locals {
  layer_zip_path    = "layer.zip"
  layer_name        = "my_lambda_requirements_layer"
  requirements_path = "${path.root}/../requirements.txt"
}

# create zip file from requirements.txt. Triggers only when the file is updated
resource "null_resource" "lambda_layer" {
  triggers = {
    requirements = filesha1(local.requirements_path)
  }
  # the command to install python and dependencies to the machine and zips
  provisioner "local-exec" {
    command = <<EOT
      set -e
      sudo apt-get update -y
      sudo apt install python3 python3-pip zip -y
      sudo rm -rf python
      sudo mkdir python
      sudo pip3 install -r ${local.requirements_path} -t python/
      zip -r ${local.layer_zip_path} python/
    EOT
  }
}

# upload zip file to s3
resource "aws_s3_object" "lambda_layer_zip" {
  bucket     = aws_s3_bucket.lambda_bucket.id
  key        = "lambda_layers/${local.layer_name}/${local.layer_zip_path}"
  source     = local.layer_zip_path
  depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
}

# create lambda layer from s3 object
resource "aws_lambda_layer_version" "my-lambda-layer" {
  s3_bucket           = aws_s3_bucket.lambda_bucket.id
  s3_key              = aws_s3_object.lambda_layer_zip.key
  layer_name          = local.layer_name
  compatible_runtimes = [var.python_runtime]
  skip_destroy = true
  depends_on          = [aws_s3_object.lambda_layer_zip] # triggered only if the zip file is uploaded to the bucket
}