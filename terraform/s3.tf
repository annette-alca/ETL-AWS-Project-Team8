#s3 Ingestion Bucket

resource "aws_s3_bucket" "ingestion_s3" {
  bucket_prefix = "team-08-ingestion-"

  tags = {
    Name        = "Ingestion bucket"
  }
}


#s3 Processed Bucket

resource "aws_s3_bucket" "processed_s3" {
  bucket_prefix = "team-08-processed-"

  tags = {
    Name        = "Processed bucket"
  }
}