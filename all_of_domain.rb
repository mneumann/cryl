base_dir = ARGV[0]
domain = ARGV[1]
system("find #{ File.join(base_dir, 'files', *domain.downcase.split('.').reverse) } -name '*.data' | xargs -I % cat %")
