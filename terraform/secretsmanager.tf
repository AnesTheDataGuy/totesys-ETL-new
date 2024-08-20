resource "aws_secretsmanager_secret" "db_credentials_" {
  name = "totesys-credentials-1234"
}

resource "aws_secretsmanager_secret_version" "db_credentials_" {
  secret_id     = aws_secretsmanager_secret.db_credentials_.id
  secret_string = jsonencode({
    DB_USERNAME = var.UN
    DB_PASSWORD = var.PW
    DB_NAME     = var.DB
    DB_HOST     = var.HT
    DB_PORT     = var.PT
  })

  depends_on = [aws_secretsmanager_secret.db_credentials_]
}