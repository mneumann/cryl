class Worker
  require 'Fetcher'
  require 'LinkExtractor'
  require 'set'

  def initialize(pid, master)
    @pid, @master = pid, master
  end

  # TODO: read from domain_entry
  OPTS = {
    :valid_content_types => [ /text\// ],
    :timeout => 3
  }

  def work
    done = Hash.new

    packet = @master.acquire_work_packet(@pid)
    if packet.empty?
      p "empty!"
      sleep 1
    end

    packet.each do |row|

      #####################################################################
      #####################################################################

      url = row['url']
      link = from_url(row['url'])
      domain_entry = @master.get_domain_entry_by_id(row['domain_entry_id'])
      same_domain_pattern = Regexp.new(domain_entry['same_domain_pattern'])

      log "crawling: #{ url }"

      err, ctype, body = Fetcher.fetch(link[0], link[1], link[2], link[3], OPTS)

      # TODO compress and store if err == :ok

      if err != :ok
        p err
        done[url] = "FAILED: #{err}" 
        next
      end

      # we have reached the maximum depth!
      if row['depth'] <= 0 
        p "depth reached"
        done[url] = "OK: depth reached"
        next
      end

      link_set = LinkExtractor.extract(body, link[0], link[1], link[2]) {|host, port, _, _|
        host =~ same_domain_pattern and port == 80
      }

      urls = []
      link_set.each {|host, port, path, query| urls << to_url(host, port, path, query) }

      @master.add_urls(urls, row['depth'].to_i - 1, domain_entry['id'])

      done[url] =  "OK"

      #####################################################################
      #####################################################################
    end

    @master.finish_work_packet(@pid, done) 
  end

  def run
    loop do
      work()
    end
  end

  def log(str)
    puts str
  end

  protected

  def from_url(url)
    sep = url.index("/")
    host = url[0, sep]
    path, query = url[sep..-1].split("?", 2)
    return host, 80, path, query
  end

  def to_url(host, port, path, query=nil)
    host + path + (query ? "?" + query : "")  
  end

end
