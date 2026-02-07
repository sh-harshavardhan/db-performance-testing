variable "config" {
  type = object({
    bucket_name = string
    tags     = map(string)
  })
}
