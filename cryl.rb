require 'fileutils'
require 'thread'
require 'digest/sha1'
require 'link_fetcher/link_fetcher'
require 'link_aggregator/link_aggregator'
require 'link_extractor/link_extractor'

class Cryl
  def initialize(root_dir, job_name, num_threads=1, keep_rejected_urls=false)
    @root_dir = root_dir
    @job_name = job_name
    @num_threads = num_threads 
    @keep_rejected_urls = keep_rejected_urls

    @storage_dir = File.join(@root_dir, "files")
    @jobs_dir = File.join(@root_dir, "jobs")

    @job_dir = File.join(@jobs_dir, job_name)
    @stages_dir = File.join(@job_dir, "stages")

    FileUtils.mkdir_p(@root_dir)
    FileUtils.mkdir_p(@storage_dir)
    FileUtils.mkdir_p(@jobs_dir)
    FileUtils.mkdir_p(@job_dir)
    FileUtils.mkdir_p(@stages_dir)

    @link_extractor = LinkExtractor.new
    @link_aggregator = LinkAggregator.new
  end

  def fetch(io, n, log_file=nil)
    if n == 1
      LinkFetcher.new(@storage_dir, log_file ? log_file % n : nil) do |fetcher|
        while line = io.gets
          line.chomp!
          fetcher.fetch(line)
        end
      end
    else
      stop = false
      mutex = Mutex.new
      (1..n).map {|t|
        Thread.new {
          LinkFetcher.new(@storage_dir, log_file ? log_file % t : nil) do |fetcher|
            loop do
              break if stop
              line = nil
              mutex.synchronize { line = io.gets unless io.eof? }
              if line
                line.chomp!
                fetcher.fetch(line)
              else
                stop = true
              end
            end
            log("#{t} ready to stop")
          end
          log("#{t} stopped")
        }
      }.each {|t| t.join}
    end
  end

  #
  # Read URLs from urls_in, convert them to a path name (using
  # digest_url),
  #
  def aggregate(urls_in, urls_out, urls_rej)
    while url = urls_in.gets
      url.chomp!
      if digest = Cryl.digest_url(url)
        base = File.join(@storage_dir, digest)
        fn = base + ".data"
        if File.exist?(fn)
          @link_aggregator.with_base_url(url) do
            @link_extractor.parse(fn) do |href|
              take, nurl = @link_aggregator.check(href)
              if take
                urls_out.puts nurl
              else
                urls_rej.puts nurl if @keep_rejected_urls
              end
            end
          end
        elsif not File.exist?(base + ".url")
          log("possibly wrong digest_url: #{fn}, #{url}")
        else
        end
      else
        log("invalid URL: #{url}")
      end
    end
  end

  def crawl(last_url_file, max_depth=2, cur_depth=0)
    log("entered method crawl #{cur_depth}/#{max_depth}")

    urls_in_name = urls_file(cur_depth, ".in")
    urls_out_name = urls_file(cur_depth, ".out")
    urls_rej_name = urls_file(cur_depth, ".rej")

    begin
      File.link(last_url_file, urls_in_name)
    rescue
      FileUtils.copy(last_url_file, urls_in_name) rescue nil
    end

    log("fetching...")
    File.open(urls_in_name, "r") do |urls_in|
      fetch(urls_in, @num_threads, File.join(stage_n(cur_depth), "fetch.log.%s"))
    end

    if cur_depth < max_depth
      log("URL extracting/aggregating...")
      File.open(urls_in_name, "r") do |urls_in|
        File.open(urls_out_name, "w+") do |urls_out|
          File.open(urls_rej_name, "w+") do |urls_rej|
            aggregate(urls_in, urls_out, urls_rej)
          end
        end
      end

      crawl(urls_out_name, max_depth, cur_depth+1)
    end
  end

  def urls_file(stage, ext=nil)
    File.join(stage_n(stage), "urls#{ext}")
  end

  def stage_n(stage)
    n = File.join(@stages_dir, stage.to_s)
    FileUtils.mkdir_p(n)
    return n
  end

  SPLIT_AT = 3

  def self.domain_to_path(host)
    list = []
    host.gsub(/[^0-9a-zA-Z.-]+/, '').split(".").reverse.each {|x|
      pos = 0
      while pos < x.size
        list << x[pos, SPLIT_AT]
        pos += SPLIT_AT
      end
      list.last << "." # to separate domain parts
    }

    list.join("/")
  end

  def self.digest_url(url)
    if url =~ /^http:\/\/([^\/]+)(.*)$/
      host_port, req_uri = $1.downcase, $2
      host, port = host_port.split(":", 2)
      port ||= 80

      if req_uri.empty? or req_uri.index("?") == 0 
        req_uri = "/" + req_uri
      end

      hex = Digest::SHA1.hexdigest("http://#{host}:#{port}#{req_uri}")

      [Cryl.domain_to_path(host), "_", hex[0,2], hex[2,2], hex[4,2], hex[6..-1]].join("/")
    else
      nil
    end
  end

end
