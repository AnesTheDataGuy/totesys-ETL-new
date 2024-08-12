variable "lambda_bucket" {
    type = string
    default = "totesys-lambda-"
}

variable "raw_data" {
    type = string
    default = "totesys-raw-data-"
}

variable "processed_data" {
    type = string
    default = "totesys-raw-data-"
}

variable "athena_queries" {
    type = string
    default = "totesys-raw-data-"
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