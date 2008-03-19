require 'fileutils'

WORK_DIR = File.expand_path(ARGV[0])
ROOT_DIR = File.join(WORK_DIR, "files")
DEPTH = ARGV[1].to_i
STAMP_FILE = File.join(WORK_DIR, "stamp")

FileUtils.mkdir_p(WORK_DIR)
FileUtils.mkdir_p(ROOT_DIR)

# for link_fetcher
ENV['CRYL_ROOT_DIR'] = ROOT_DIR
ENV['CRYL_ERROR_LOG'] = File.join(WORK_DIR, 'fetch.log')

def urls_file(depth)
  File.join(WORK_DIR, "urls.#{depth}")
end

def find_newer(pattern, depth)
  if depth == 0
    "find #{ROOT_DIR} -name '#{pattern}'"
  else
    "find #{ROOT_DIR} -name '#{pattern}' -newer #{STAMP_FILE}"
  end
end

system("cd link_fetcher && make compile")
system("cd link_extractor && make")

# create initial urls file.
FileUtils.cp(ARGV[2], urls_file(0)) 

DEPTH.times do |depth|
  `touch #{STAMP_FILE}` # used to find all files that are newer than this file.

  puts "Depth: #{depth}"

  Dir.chdir('./link_fetcher') do 
    system("./link_fetcher < #{ urls_file(depth) }")
  end
  puts "Fetching done!"

  system("#{ find_newer('*.data', depth) } | ./link_extractor/link_extractor")
  puts "Extracting done!"

  system("#{ find_newer('*.links', depth) } | ruby ./link_aggregator/link_aggregator.rb > #{urls_file(depth+1)}")
end
