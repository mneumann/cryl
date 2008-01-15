require 'rubygems'
require 'Master'
require 'Worker'

PACKET_SIZE = 10

master = Master.new('master.db', PACKET_SIZE)

master.add('www.ntecs.de', 
           :same_domain_pattern => /(^|[.])ntecs[.]de$/,
           :timeout => 5,
           :valid_content_types => [/text\//],
           :max_depth => 5)

master.add('www.heise.de',
           :same_domain_pattern => /(^|[.])heise[.]de$/,
           :timeout => 3,
           :valid_content_types => [/text\/html/],
           :max_depth => 10,
           :start_url => 'www.heise.de/')


threads = []
5.times do |t|
  threads << Thread.new { Worker.new("p#{t}", master, './work').run }
end
threads.each do |t| t.join end
