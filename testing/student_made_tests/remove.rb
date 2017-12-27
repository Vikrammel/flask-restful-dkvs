#!/usr/bin/env ruby

remove_num = ARGV[0]
index = 0

while index < remove_num.to_i
  system 'docker kill node' + index.to_s
  system 'docker rm node' + index.to_s
  index = index + 1
end
