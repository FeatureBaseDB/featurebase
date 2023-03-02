package featurebase.authz

default allow = false

allow {
    input.method = "SELECT"
    input.index = "index_a"
    input.user = "fletcher"
}

