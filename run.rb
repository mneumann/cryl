require 'rubygems'
require 'Master'
require 'Worker'

master = Master.new('/tmp/real_test_master.db', 10)
id = master.add_domain_entry('ntecs.de', '(^|[.])ntecs[.]de$', 5) 
p id
p master.add_urls(['www.ntecs.de/'], 5, id)

#id = master.add_domain_entry('heise.de', '(^|[.])heise[.]de$', 100) 
id = master.add_domain_entry('heise.de', '', 100) 
p id
p master.add_urls(['www.heise.de/'], 100, id)



threads = []
5.times do |t|
  threads << Thread.new { Worker.new("p#{t}", master, './work').run }
end
threads.each do |t| t.join end
