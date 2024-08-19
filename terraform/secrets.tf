resource "aws_secretsmanager_secret" "totesys_secret" {
  name = "totesys_database_credentials_test"
  description = "secret for database"
  
}

variable "credentials" {
  default = {
  }
  type = map(string)
}

resource "aws_secretsmanager_secret_version" "totesys_secret_manager" {
  secret_id     = aws_secretsmanager_secret.totesys_secret.id
  secret_string = jsonencode(var.credentials)
}
