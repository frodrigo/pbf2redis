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

# Loop over ways having the tag "highway"
redis.sscan_each('Way:Key:highway->wids') { |wid|
  # Fetch nodes id of the way
  nids = redis.smembers("Way:#{wid}->nids")
  # Add way node ids to register set collecting them
  redis.sadd(REG1, nids)
}

# Loop over ways having the tag "power"
redis.sscan_each('Way:Key:power->wids') { |wid|
  # Check if it is a powerline
  if [d_line, d_minor_line, d_cable].include?(redis.hget("Way:#{wid}->tags", d_power))
    # Fetch nodes id of the way
    nid = redis.smembers("Way:#{wid}->nids")
    # Then add way node ids to the register set collecting them
    redis.sadd(REG2, nid)
  end
}

# Intersects the two node ids set and store the result into the register REG3
redis.sinterstore(REG3, REG1, REG2)

# Ouput the node ids from the result register
redis.sscan_each(REG3) { |nid|
  puts nid
}
