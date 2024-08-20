variable "lambda_bucket" {
  type    = string
  default = "totesys-lambda-"
}

variable "raw_data" {
  type    = string
  default = "totesys-raw-data-"
}

variable "processed_data" {
  type    = string
  default = "totesys-processed-data-"
}

variable "athena_queries" {
  type    = string
  default = "totesys-athena-queries-"
}

variable "extract_lambda" {
  type    = string
  default = "extract"
}

variable "transform_lambda" {
  type    = string
  default = "transform"
}

variable "load_lambda" {
  type    = string
  default = "load"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

# variables for secrets manager
variable "UN" {
  type = string
}

variable "PW" {
  type = string
}

variable "DB" {
  type = string
}

variable "HT" {
  type = string
}
variable "PT" {
  type = string
}

