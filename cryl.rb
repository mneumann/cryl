require 'fileutils'

WORK_DIR = File.expand_path(ARGV[0])
ROOT_DIR = File.join(WORK_DIR, "files")
DEPTH = ARGV[1].to_i

FileUtils.mkdir_p(WORK_DIR)
FileUtils.mkdir_p(ROOT_DIR)

# for link_fetcher
ENV['CRYL_ROOT_DIR'] = ROOT_DIR
ENV['CRYL_ERROR_LOG'] = File.join(WORK_DIR, 'fetch.log')

def urls_file(depth)
  File.join(WORK_DIR, "urls.#{depth}")
end

# create initial urls file.
FileUtils.cp(ARGV[2], urls_file(0)) 

DEPTH.times do |depth|
  puts "Depth: #{depth}"
  Dir.chdir('./link_fetcher') do 
    system("./link_fetcher < #{urls_file(depth)}")
  end
  puts "Fetching done!"

  system(%{find #{ROOT_DIR} -name '*.data' | ./link_extractor/link_extractor})
  puts "Extracting done!"

  system("ruby ./link_aggregator/link_aggregator.rb #{ROOT_DIR} > #{urls_file(depth+1)}")
end
