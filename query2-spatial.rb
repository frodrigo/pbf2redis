require 'redis'

REG1 = 'reg1'
REG2 = 'reg2'
REG3 = 'reg3'
REG4 = 'reg4'

# https://github.com/redis/redis-rb
redis = Redis.new

tile38 = Redis.new(port: 9851)

redis.del(REG1)
redis.del(REG2)
redis.del(REG3)
redis.del(REG4)

d_power, d_line, d_minor_line, d_cable = redis.hmget('Dictionary:string->index', ['power', 'line', 'minor_line', 'cable'])

# analyser_osmosis_highway_bad_intersection.py

sadd_smembers = redis.script(:load, "
redis.call('sadd', KEYS[1], unpack(redis.call('smembers', KEYS[2])))
")

redis.sscan_each('Way:Key:power->wids') { |wid|
  if [d_line, d_minor_line, d_cable].include?(redis.hget("Way:#{wid}->tags", d_power))

    cursor = 0
    redis.pipelined do
      redis.evalsha(sadd_smembers, [REG2, "Way:#{wid}->nids"])
      wids = []
      begin
        cursor, inter_wids = tile38.call(:INTERSECTS, 'Way_Index:Tag:highway->wid', :CURSOR, cursor, :ids, :get, 'Way_Index:Tag:power->wid', wid)
        wids += inter_wids
      end while cursor != 0
      wids.each { |wid|
        redis.evalsha(sadd_smembers, [REG3, "Way:#{wid}->nids"])
      }
    end
  end
}

redis.sinterstore(REG4, REG2, REG3)

redis.sscan_each(REG4) { |nid|
  puts nid
}
