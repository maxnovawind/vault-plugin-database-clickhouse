length = 20

rule "charset" {
  charset = "abcdefghijklmnopqrstuvwxyz"
}

rule "charset" {
  charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  min-chars = 1
}

rule "charset" {
  charset = "0123456789"
  min-chars = 1
}