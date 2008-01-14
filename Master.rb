#
# The master maintains the central database.
#
class Master
  require 'sqlite3'
  require 'thread'

  def initialize(db_file, init_db=false)
    @db_mutex = Mutex.new
    @db = SQLite3::Database.new(db_file)
    @db.results_as_hash = true
    @db.type_translation = true
    init_db! if init_db
  end

  #
  # Add a new +url+ to be fetched +not_before+.
  #
  def add_url(url, depth=0, not_before=Time.now)
    raise ArgumentError if depth < 0
    @db_mutex.synchronize do
      begin
        @db.execute('INSERT INTO fetches (url, depth, not_before) VALUES (?,?,?)', url, depth, not_before)
        true
      rescue SQLite3::SQLException
        false
      end
    end
  end

  def add_work_packet()
    'url1' => depth
  end

  #
  # pid:: Worker processor identificator, a String.
  #
  def acquire_work_packet(pid, packet_size=50) 
    raise ArgumentError if packet_size > 500
    sql_find = "SELECT * FROM fetches WHERE processed_by IS NULL AND not_before < ? LIMIT #{packet_size}"
    sql_updt = "UPDATE fetches SET processed_by = ?, acquired_at = ? WHERE url = ?"  
    urls = {}

    @db_mutex.synchronize do
      @db.transaction do
        updt = @db.prepare(sql_updt)
        @db.execute(sql_find, Time.now).each {|row|
          updt.execute(pid, Time.now, row['url'])
          urls[row['url']] = row['depth']
        }
      end
    end

    return urls
  end

  # each fetch entry must have a fetch_depth associated.
  #
  # a worker doesn't returns urls if the depth reaches 0. 
  #
  # return a hash of
  #   {url1 => depth, url2 => depth, url3 => depth}
  #
  #   add_urls
  #   finish_work_packet(url1, url2, url3, statuses)
  #   for each domain, the master must keep a global not_before time
  #   which it uses.
  #
  #   * As domains are not that frequent, it might cache them in memory.
  #
  #   * Upon restart, this has to be determined by select 
  #
  #   * each entry in table fetches must be associated with a DomainEntry
  #     to lookup the information there.
  #
  # domain_entries ( domain, same_domain_pattern, max_depth ) 
  #
  # domain_statistics ( domain )   
  #   fetch entry for domain. read max_depth settings etc.
  #   update statistics for domain. 
  #
  # domain_statistics could be cached in memory and recalculated upon
  # restart.
  #
  #   fetches: time_spent
  #   fetches: download 
  #
  #   {url1 => depth1, ...}
  #   finish_work_packet( :url1 => [time_spent, bytes] )
  #

  #
  # Packet: {'url1' => "OK", 'url2' => "FAILED"}
  #
  def finish_work_packet(pid, packet)
    sql_updt = "UPDATE fetches SET processed_status = ?, released_at = ? WHERE url = ? AND processed_by = ?"  

    @db_mutex.synchronize do
      @db.transaction do
        updt = @db.prepare(sql_updt)
        packet.each do |url, status|
          updt.execute(status, Time.now, url, pid)  
        end
      end
    end
  end

  protected

  def init_db!
    @db.execute %{
      CREATE TABLE fetches (
        url VARCHAR(2048) PRIMARY KEY NOT NULL,
        not_before TIMESTAMP NOT NULL,
        depth INTEGER NOT NULL DEFAULT 0,
        processed_by VARCHAR(50), -- who is working on it right now
        processed_status VARCHAR(50),
        acquired_at TIMESTAMP,
        released_at TIMESTAMP
      );
    }
    @db.execute %{
      CREATE INDEX fetches_not_before_idx ON fetches (not_before);
    }
    @db.execute %{
      CREATE INDEX fetches_processed_by_idx ON fetches (processed_by);
    }
  end
end

if __FILE__ == $0
  m = Master.new('/tmp/master.db', false)
  p m.add_url('hallo_leute', 2) 
  p m.add_url('hallo_leute2') 
  p m.add_url('hallo_leute') 
  p m.add_url('blah') 
  p m.acquire_work_packet('abc')
  p m.acquire_work_packet('def')
  p m.finish_work_packet('abc', 'hallo_leute' => 'OK')
end
