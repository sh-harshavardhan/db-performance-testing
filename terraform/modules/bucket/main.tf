
resource "aws_s3_bucket" "bucket"{
  bucket = var.config.bucket_name
  tags = var.config.tags
}

data "template_file" "iam_policy_template" {
  template = file("${path.module}/bucket-policy.json.tmpl")
  vars = {
    bucket_arn = aws_s3_bucket.bucket.arn
  }
}

resource "aws_s3_bucket_policy" "bucket_policy" {
  bucket = aws_s3_bucket.bucket.id
  policy = data.template_file.iam_policy_template.rendered
}
