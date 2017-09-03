require 'redis'

REG1 = 'reg1'
REG2 = 'reg2'
REG3 = 'reg3'
REG4 = 'reg4'

# https://github.com/redis/redis-rb
redis = Redis.new

redis.del(REG1)
redis.del(REG2)
redis.del(REG3)
redis.del(REG4)

# analyser_osmosis_highway_features.py

# Get all node with railway=level_crossing or railway=crossing and store it in register REG1
redis.sunionstore(REG1, 'Node:Key:railway:Value:level_crossing->nids', 'Node:Key:railway:Value:crossing->nids')

# For each node
redis.sscan_each(REG1) { |nid|
  r = redis.pipelined {
    # Intersect the list of ways ids using this node with the list of ways using highway and railway tags, sotre in registers REG2 and REG3
    redis.sinterstore(REG2, "Node:#{nid}->wids", 'Way:Key:highway->wids')
    redis.sinterstore(REG3, "Node:#{nid}->wids", 'Way:Key:railway->wids')

    # Count the number of ways in each register and returns it
    redis.scard(REG2)
    redis.scard(REG3)
  }[-2..-1].any?(&:zero?) # Any of the return value are zero ? ie: there is no highway nor railway ?
  if r
    # Ouput the node id
    puts nid
  end
}
