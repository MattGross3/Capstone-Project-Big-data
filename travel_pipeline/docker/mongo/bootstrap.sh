#!/bin/bash
set -euo pipefail

sleep 8

mongosh --host configsvr:27019 <<'EOF'
rs.initiate({_id: "cfg", configsvr: true, members: [{ _id: 0, host: "configsvr:27019" }]})
EOF

sleep 8

mongosh --host shard1:27018 <<'EOF'
rs.initiate({_id: "rs-shard-01", members: [{ _id: 0, host: "shard1:27018" }]})
EOF

mongosh --host shard2:27020 <<'EOF'
rs.initiate({_id: "rs-shard-02", members: [{ _id: 0, host: "shard2:27020" }]})
EOF

sleep 8

mongosh --host mongos:27017 <<'EOF'
sh.addShard("rs-shard-01/shard1:27018")
sh.addShard("rs-shard-02/shard2:27020")
sh.enableSharding("travel_ops")
sh.shardCollection("travel_ops.flights_raw", { "FL_DATE": 1, "OP_UNIQUE_CARRIER": 1 })
EOF

echo "MongoDB sharded cluster initialized."
