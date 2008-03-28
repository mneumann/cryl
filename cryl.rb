require 'fileutils'
require 'thread'
require 'link_fetcher/link_fetcher'
require 'link_aggregator/link_aggregator'
require 'link_extractor/link_extractor'

class Cryl
  def initialize(root_dir, num_threads=1, keep_rejected_urls=false)
    @root_dir = root_dir
    @num_threads = num_threads 
    @keep_rejected_urls = keep_rejected_urls

    @storage_dir = File.join(@root_dir, "files")
    @stages_dir = File.join(@root_dir, "stages")
    @fetch_log = File.join(@root_dir, "fetch.log")

    FileUtils.mkdir_p(@root_dir)
    FileUtils.mkdir_p(@storage_dir)
    FileUtils.mkdir_p(@stages_dir)

    @link_extractor = LinkExtractor.new
    @link_aggregator = LinkAggregator.new
  end

  attr_accessor :fetch_log

  def bulk_fetch(io, log_file=nil)
    LinkFetcher.new(@storage_dir, log_file) do |fetcher|
      while line = io.gets
        line.chomp!
        fetcher.fetch(line)
      end
    end
  end

  def bulk_fetch_threaded(io, n, log_file=nil)
    mutex = Mutex.new

    (1..n).map {|t|
      Thread.new {
        LinkFetcher.new(@storage_dir, log_file ? log_file % t : nil) do |fetcher|
          stop = false
          while not stop
            mutex.synchronize do
              if !io.eof? and line = io.gets
                line.chomp!
                fetcher.fetch(line)
              else
                stop = true
              end
            end
          end
          log("#{t} ready to stop")
        end
        log("#{t} stopped")
      }
    }.each {|t| t.join}
  end

  def cycle(urls_in, urls_out, urls_rej=nil, not_before=Time.now)
    log("fetching...")
    if @num_threads == 1
      bulk_fetch(urls_in, @fetch_log)
    else
      bulk_fetch_threaded(urls_in, @num_threads, @fetch_log + ".%s")
    end

    log("URL extracting/aggregating...")
    Dir.glob("#{@storage_dir}/**/*.url") do |fn|
      next if not_before and File.stat(fn).mtime < not_before
      @link_aggregator.with_base_url(File.read(fn)) do
        @link_extractor.parse(fn[0..-4] + "data") do |href|
          take, url = @link_aggregator.check(href)
          if take
            urls_out.puts url
          else
            urls_rej.puts url if urls_rej
          end
        end
      end
    end
  end

  def crawl(last_url_file, max_depth=2, cur_depth=0, update=false)
    log("entered method crawl #{cur_depth}/#{max_depth}")
    next_url_file = crawl_cycle(last_url_file, cur_depth, (update && cur_depth == 0) ? nil : Time.now)
    crawl(next_url_file, max_depth, cur_depth+1) if cur_depth < max_depth
  end

  def crawl_cycle(last_url_file, stage, not_before=nil) 
    urls_in_name = urls_file(stage, ".in")
    urls_out_name = urls_file(stage, ".out")
    urls_rej_name = urls_file(stage, ".rej")

    begin
      File.link(last_url_file, urls_in_name)
    rescue
      FileUtils.copy(last_url_file, urls_in_name)
    end

    self.fetch_log = File.join(stage_n(stage), "fetch.log")

    File.open(urls_in_name, "r") do |urls_in|
      File.open(urls_out_name, "w+") do |urls_out|
        File.open(urls_rej_name, "w+") do |urls_rej|
          cycle(urls_in, urls_out, @keep_rejected_urls ? urls_rej : nil, not_before)
        end
      end
    end
    return urls_out_name
  end

  def urls_file(stage, ext=nil)
    File.join(stage_n(stage), "urls#{ext}")
  end

  def stage_n(stage)
    n = File.join(@stages_dir, stage.to_s)
    FileUtils.mkdir_p(n)
    return n
  end

end
