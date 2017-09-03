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

d_power, d_line, d_minor_line, d_cable = redis.hmget('Dictionary:string->index', ['power', 'line', 'minor_line', 'cable'])

# analyser_osmosis_highway_bad_intersection.py

# Load a Lua script to append to register set the content of an other set
sadd_smembers = redis.script(:load, "
redis.call('sadd', KEYS[1], unpack(redis.call('smembers', KEYS[2])))
")

### Loop over ways having the tag "highway"
#redis.sscan_each('Way:Key:highway->wids') { |wid|
#  # Append node ids of the way to register set collecting them
#  redis.evalsha(sadd_smembers, [REG1, "Way:#{wid}->nids"])
#}

# Loop over ways having the tag "power"
redis.sscan_each('Way:Key:power->wids') { |wid|
  # Check if it is a powerline
#  if ['line', 'minor_line', 'cable'].include?(redis.hget("Way:#{wid}->tags", 'power'))
  if [d_line, d_minor_line, d_cable].include?(redis.hget("Way:#{wid}->tags", d_power))
    # Append node ids of the way to register set collecting them
    redis.evalsha(sadd_smembers, [REG2, "Way:#{wid}->nids"])
  end
}

# Intersects the two node ids set and store the result into the register REG3
redis.sinterstore(REG3, 'Way:Key:highway->nids', REG2)

# Ouput the node ids from the result register
redis.sscan_each(REG3) { |nid|
  puts nid
}
