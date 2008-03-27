#
# Interface to Erlang link_fetcher process.
#
class LinkFetcher
  DIR = File.expand_path(File.dirname(__FILE__))
  BINARY = File.join(DIR, "link_fetcher")

  attr_reader :root_dir, :log_file

  def initialize(root_dir=nil, log_file=nil)
    @root_dir = root_dir
    @log_file = log_file
    ENV['CRYL_ROOT_DIR'] = @root_dir if @root_dir
    ENV['CRYL_ERROR_LOG'] = @log_file if @log_file
    ENV['ERL_AFLAGS'] = "-pa #{DIR}"
    IO.popen(BINARY, 'w') {|@io|
      yield self
    }
  end

  def fetch(url)
    @io.puts url
  end
end

if __FILE__ == $0
  LinkFetcher.new do |l|
    1_000.times do
      l.fetch("http://www.google.de/")
    end
  end
end
