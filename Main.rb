require 'NodeManager'
require 'HttpUrl'
require 'VictimCache'
require 'Scheduler'

def enqueue_list(node_manager, fh, nlines=nil)
  i = 0
  while url = fh.gets
    if http_url = HttpUrl.parse(url)
      unless node_manager.has_file?(http_url.to_filename)
        node_manager.enqueue_url(http_url.to_s)
      end
    else
      node_manager.log_error :invalid_url, url.inspect
    end
    i += 1
    break if nlines and i >= nlines
  end
end

if __FILE__ == $0
  node_manager = NodeManager.new('./work')
  node_manager.setup!
  enqueue_list(node_manager, File.open('/tmp/adbrite-urls.txt'), 10_000)

  scheduler = Scheduler.new(node_manager, 1000)
  scheduler.run
end
