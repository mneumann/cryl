#
# Code for managing one node, including queues etc. 
#
# Copyright (c) 2008 by Michael Neumann (mneumann@ntecs.de)
#
class NodeManager
  require 'fileutils'
  require 'digest/sha1'

  #
  # Each node has it's own directory root in which it stores several
  # different files. For example downloaded files go into files/ and the
  # queue files are managed in queues/.
  #
  attr_reader :root_dir

  attr_reader :files_dir

  def initialize(root_dir, num_queues=213)
    @num_queues = num_queues
    @root_dir = File.expand_path(root_dir)

    @files_dir = File.join(@root_dir, 'files')

    @queues_dir = File.join(@root_dir, 'queues')
    @current_queue_dir = File.join(@queues_dir, 'current')
    @next_queue_dir = File.join(@queues_dir, 'next')
    @queue_version = File.join(@queues_dir, 'version')

    @next_queue_files = Array.new(@num_queues)
    @current_queue_file = nil 

    @logs_dir = File.join(@root_dir, 'logs')
    @error_log = File.join(@logs_dir, 'error.log')
    @action_log = File.join(@logs_dir, 'action.log')

    # 
    # Temporary files
    #
    @tmps_dir = File.join(@root_dir, 'tmps')
    @tmps_cnt = 0
  end

  #
  # Create the initial directory structure
  #
  def setup!
    FileUtils.rm_rf(@root_dir) if File.exist?(@root_dir)
    FileUtils.mkdir_p(@root_dir)
    FileUtils.mkdir_p(@files_dir)
    FileUtils.mkdir_p(@queues_dir)
    FileUtils.mkdir_p(@logs_dir)
    FileUtils.mkdir_p(@current_queue_dir)
    FileUtils.mkdir_p(@next_queue_dir)
    FileUtils.mkdir_p(@tmps_dir)

    File.open(@error_log, 'w+') {|f|}
    File.open(@action_log, 'w+') {|f|}

    # the queue_version file contains the number of
    # switches from current->next queue.
    File.open(@queue_version, 'w+') {|f| f.puts "0"}
  end

  #
  # Enqueue always works on the 'next' queue, while
  # from the current queue elements are only dequeued.
  #
  def enqueue_url(url)
    raise "FATAL: invalid URL" if url =~ /[\n\r]/
    queue_no = select_queue(url)
    fh = (@next_queue_files[queue_no] ||= File.open(File.join(@next_queue_dir, queue_no.to_s), 'a+'))
    fh.puts url
  end

  def dequeue_url
    switches = 0 # number of queue switches
    while switches < 2 
      if @current_queue_file
        if line = @current_queue_file.gets
          line.chomp!
          return line
        else
          # eof
          @current_queue_file.close
          File.delete(@current_queue_filename)
          @current_queue_file = nil
          @current_queue_filename = nil
        end
      else
        if fn = Dir[File.join(@current_queue_dir, '*')].first
          @current_queue_file = File.open(fn, 'r')
          @current_queue_filename = fn 
        else
          # no more files in current queue 
          # make "next" the current and try again
          switch_queues()
          switches += 1
        end
      end
    end
    return nil # all queues are empty
  end

  def switch_queues
    @next_queue_files.each do |fh|
      fh.close if fh
    end
    @next_queue_files = Array.new(@num_queues)
    @current_queue_file.close if @current_queue_file

    return if Dir[File.join(@next_queue_dir, '*')].empty? and 
              Dir[File.join(@current_queue_dir, '*')].empty?

    File.rename(@current_queue_dir, @current_queue_dir + ".rename")
    File.rename(@next_queue_dir, @current_queue_dir) 
    File.rename(@current_queue_dir + ".rename", @next_queue_dir)

    version = File.read(@queue_version).to_i + 1
    File.open(@queue_version, 'w+') {|f| f.puts version.to_s }
  end

  def request_temp_filename
    @tmps_cnt += 1
    fn = File.join(@tmps_dir, @tmps_cnt.to_s)
    raise "FATAL" if File.exist?(fn)
    return fn
  end

  def shutdown
    @next_queue_files.each do |fh|
      fh.close if fh
    end
    @current_queue_file.close if @current_queue_file
  end

  private

  def select_queue(seed_string=nil)
    q = 0 
    if seed_string
      q += Digest::SHA1.digest(seed_string).unpack("I*").inject(0) {|i,s| i+s}
    end
    q += rand(@num_queues)
    return q % @num_queues
  end
end