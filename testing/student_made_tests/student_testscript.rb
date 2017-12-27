#!/usr/bin/env ruby

num_clusters = ARGV[0].to_i
k_number = ARGV[1]
starting_port = 8181
base_ip = '10.0.0.'

port_ary = Array.new(num_clusters)
ipport_ary = Array.new(num_clusters)

system 'docker build -t hw4 .'

starting_view = ''
index = 0
x = 51
while index < num_clusters
  starting_view << base_ip << x.to_s << ':8080,'
  port_ary[index] = starting_port + index
  ipport_ary[index] = base_ip + x.to_s
  index = index + 1
  x = x + 1
end

index = 0
while index < num_clusters

  string = 'docker run -p ' << port_ary[index].to_s << ':8080 --ip=' << ipport_ary[index] << ' --net=mynet -e K='
  string << k_number << ' -e VIEW="' << starting_view << '" -e IPPORT="' << ipport_ary[index] << ':8080" --name '
  string << 'node' << index.to_s << ' hw4'
  spawn string
  index = index + 1
end
