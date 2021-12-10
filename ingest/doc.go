// Package ingest provides tooling for accepting record-oriented data updates
// and converting them to data that can be efficiently merged into stored
// data. Nia's original description:
//
// but the overall pipeline is:
// 1. fetch the schema and use it to configure the codec
// 2. parse the data with the codec into vectors, while stuffing temp record key mappings into a string table
// 3. call *CreateKeys on the cluster for all of the things
// 4. generate an ID remapping table for record keys and apply it to all of the vectors
// 5. remap the string keys
// 6. group each vector by shard
// 7. convert the shard vectors into matrix updates
// 8. combine those matrix updates into a shard update
// 9. send the shard updates out over the internal client
// 10. the nodes apply them to RBF
package ingest
