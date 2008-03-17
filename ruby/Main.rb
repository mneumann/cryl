require 'NodeManager'
require 'HttpUrl'
require 'VictimCache'
require 'HttpClient'
require 'FetchManager'

if __FILE__ == $0
  HttpUrl.dns_cache = VictimCache.new(10000) 

  node_manager = NodeManager.new('./work')
  node_manager.setup!

  p node_manager.enqueue_lines(File.open('/home/mneumann/adbrite-urls.txt'), 10_000)

  fetch_manager = FetchManager.new(node_manager)
  fetch_manager.run
end
