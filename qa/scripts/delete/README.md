

# backup command
featurebase backup --index tremor -o tremor-backup --concurrency 64 

# data was tar in parallel with this command
time tar cfv - ./tremor-backup/ | pigz -1 -p 64 > tremor.tar.gz