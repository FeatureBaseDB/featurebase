#!/bin/bash

## tournament.sh runs a sequence of duels between greens and blues.
## Each test run changes the PILOSA_TXSRC and runs either
## one or two backends through the rigors of make testv-race.
## logs are saved to the tourna.log.${i} files.

for i in rbf lmdb roaring bolt rbf_lmdb rbf_roaring lmdb_rbf lmdb_roaring roaring_rbf roaring_lmdb roaring_bolt lmdb_bolt; do
   echo "$(date) starting ${i}, output to tourna.log.${i}"
   echo "***=== ${i} ====================*** $(date)" &> tourna.log.${i}
   PILOSA_TXSRC=${i}  make testv-race 2>&1 > tourna.log.${i}
done

