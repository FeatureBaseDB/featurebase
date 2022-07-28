## Ingester

_Ingester service_ consumes [Programmatic Ingest API](https://github.com/molecula/docs/blob/master/docs/internal/proposals/programmatic-ingest-api.md)
and sends data to the _Pilosa service_.

```
Usage of ./ingester:
      --dry-run                Dry run - just flag parsing.
      --http-addr string       HTTP address for Ingester. (default "localhost:8080")
      --pilosa-hosts strings   Comma separated list of host:port pairs for Pilosa. (default [localhost:10101])

```
