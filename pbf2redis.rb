require 'pbf_parser'
require 'redis'

# https://github.com/redis/redis-rb
redis = Redis.new
puts 'FLUSHALL...'
redis.flushall

# https://github.com/tidwall/tile38
require 'redic'
#tile38 = Redic.new('redis://localhost:9851')
tile38 = Redis.new(port: 9851)
puts 'FLUSHDB...'
tile38.call(:flushDB)


index_key_value = ['railway']
index_key = index_key_value + ['highway', 'power']

@dictionary = {'lon': 0, 'lat': 1}
@dict_index = 2

def dict_encode(k)
  i = @dictionary[k]
  if !i
    i = @dict_index += 1
    @dictionary[k] = i
  end
  i
end

def dict_encode_hash(h)
  r = []
  h.each{ |k, v|
    r << dict_encode(k)
    r << dict_encode(v)
  }
  r
end

def dict_decode(ik)
  @dictionary[ik]
end

def dict_decode_hash(ih)
  Hash[ih.collect{ |kh, vh|
    [dict_decode(kh.to_i), dict_decode(vh.to_i)]
  }]
end

# https://github.com/planas/pbf_parser
pbf = PbfParser.new(ARGV[0])
blocks = pbf.size
block = 1
pbf.each do |nodes, ways, relations|
  redis.pipelined do
    unless nodes.empty?
      puts "Parse nodes #{block}/#{blocks}..."
      nodes.each do |node|
      # {:id=>5046302779, :lat=>43.7269982, :lon=>7.4195437, :version=>1, :timestamp=>1503234535000, :changeset=>51276855, :uid=>127573, :user=>"wilda69", :tags=>{}}
        redis.hmset("Node:#{node[:id]}->tags", *([0, node[:lon], 1, node[:lat]] + dict_encode_hash(node[:tags])))# unless node[:tags].empty?
        node[:tags].each{ |k, v|
          redis.sadd "Node:Key:#{k}->nids", node[:id] if index_key.include?(k)
          redis.sadd "Node:Key:#{k}:Value:#{v}->nids", node[:id] if index_key_value.include?(k)
        }
      end
    end

    unless ways.empty?
      puts "Parse ways #{block}/#{blocks}..."
      ways.each do |way|
      # {:id=>515353483, :version=>1, :timestamp=>1502658010000, :changeset=>51092803, :uid=>68661, :user=>"PeetTheEngineer", :tags=>{"name"=>"Le Verdon", "waterway"=>"stream"}, :refs=>[1026089074, 2897347919]}
        redis.hmset("Way:#{way[:id]}->tags", *dict_encode_hash(way[:tags])) unless way[:tags].empty?
        redis.sadd("Way:#{way[:id]}->nids", way[:refs]) unless way[:refs].empty?
        way[:tags].each{ |k, v|
          redis.sadd "Way:Key:#{k}->wids", way[:id] if index_key.include?(k)
          redis.sadd "Way:Key:V#{k}:Value:#{v}->wids", way[:id] if index_key_value.include?(k)
        }
      end
    end

    unless relations.empty?
      puts "Parse relations #{block}/#{blocks}..."
      relations.each do |relation|
      # {:id=>7360696, :version=>1, :timestamp=>1498599343000, :changeset=>49874058, :uid=>4763179, :user=>"ika-chan!", :tags=>{"local_ref"=>"Princesse Antoinette", "name"=>"Princesse Antoinette", "network"=>"Autobus de Monaco", "operator"=>"Compagnie des Autobus de Monaco", "public_transport"=>"stop_area", "public_transport:version"=>"2", "type"=>"public_transport"}, :members=>{:nodes=>[{:id=>1770577832, :role=>"stop"}, {:id=>4939161314, :role=>""}], :ways=>[{:id=>503645009, :role=>"platform"}], :relations=>[]}}
        redis.hmset("Rel:#{relation[:id]}->tags", *dict_encode_hash(relation[:tags])) unless relation[:tags].empty?
#        redis.lpush "WMN#{relation[:id]}", relation[:members][:nodes]
#        redis.lpush "WMN#{relation[:id]}", relation[:members][:ways]
#        redis.lpush "WMN#{relation[:id]}", relation[:members][:relations]
        relation[:tags].each{ |k, v|
          redis.sadd "Rel:Key:#{k}->rids", relation[:id] if index_key.include?(k)
          redis.sadd "Rel:Key:#{k}:Value:#{v}->rids", relation[:id] if index_key_value.include?(k)
        }
      end
    end
  end

  block += 1
end

puts "Save dictionary... (#{@dictionary.size} keys)"
redis.hmset('Dictionary:string->index', *@dictionary.to_a.flatten)
a = []
@dictionary = Hash[@dictionary.collect{ |k, v|
  a << v
  a << k
  [v, k]
}]
redis.hmset('Dictionary:index->string', *a)

puts 'Index way geom...'

index_key.each { |key|
  puts "  way #{key}..."
  tile38.pipelined do
    redis.sscan_each("Way:Key:#{key}->wids") { |wid|
      nids = redis.sscan_each("Way:#{wid}->nids").to_a
      r = redis.pipelined do
        redis.sadd "Way:Key:#{key}->nids", nids
        nids.each{ |nid|
          redis.sadd("Node:#{nid}->wids", wid)
          redis.hmget("Node:#{nid}->tags", ['0', '1'])
        }
      end
      lons_lats = r[1..-1].each_slice(2).collect(&:last)

      lon_min, lon_max = lons_lats.minmax_by(&:first).collect(&:first)
      lat_min, lat_max = lons_lats.minmax_by(&:last).collect(&:last)

      tile38.call(:set, "Way_Index:Tag:#{key}->wid", wid, :bounds, lat_min, lon_min, lat_max, lon_max)
    }
  end
}
