class Worker
  require 'Fetcher'
  require 'LinkExtractor'
  require 'set'
  require 'digest/sha1'
  require 'fileutils'
  require 'yaml'

  def initialize(pid, master, storage_dir)
    @pid, @master, @storage_dir = pid, master, storage_dir
  end

  def work
    done = Hash.new

    packet = @master.acquire_work_packet(@pid)
    sleep 5 if packet.empty?

    packet.each do |row|

      #####################################################################
      #####################################################################

      url = row['url']
      link = from_url(row['url'])
      domain = @master.get_domain_by_id(row['domain_id'])
      prefs = YAML.load(domain['prefs'])

      log "crawling: #{ url }"

      err, ctype, body = Fetcher.fetch(link[0], link[1], link[2], link[3], prefs)

      if err == :ok
        # store
        store(url, body)
      else
        log "ERROR(fetch): #{err}"
        done[url] = "FAILED: #{err}" 
        next
      end

      # we have reached the maximum depth!
      if row['depth'] <= 0 
        log "NOTE: depth reached"
        done[url] = "OK: depth reached"
        next
      end

      #
      # Extract links
      #
      same_domain_pattern = prefs[:same_domain_pattern] || //

      link_set = LinkExtractor.extract(body, link[0], link[1], link[2]) {|host, port, _, _|
        host =~ same_domain_pattern and port == 80
      }

      urls = []
      link_set.each {|host, port, path, query| urls << to_url(host, port, path, query) }

      @master.add_urls(urls, row['depth'] - 1, domain['id'])

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

  protected

  def store(url, body)
    path = url_to_path(url)
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, 'w+') {|f|
      f.puts "# url: http://#{url}\tts: #{Time.now.to_i}" 
      f.write body
    }
  end

  def url_to_path(url)
    #arr = Digest::SHA1.hexdigest(url).scan(/../)
    #File.join(@storage_dir, arr.shift, arr.shift, arr.join(""))
    File.join(@storage_dir, Digest::SHA1.hexdigest(url))
  end

  def log(str)
    puts str
  end

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
