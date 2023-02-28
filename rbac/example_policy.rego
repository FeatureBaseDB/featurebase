package featurebase.authz

default allow = false

allow {
    input.method = "SELECT"
    input.path = "index_a"
    input.user = "fletcher"

}