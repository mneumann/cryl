#
# The master maintains the central database.
#
class Master
  require 'sqlite3'
  require 'thread'

  PACKET_SIZE = 20
  SCHEMA_VERSION = 1

  def initialize(db_file)
    @db_mutex = Mutex.new
    @db = SQLite3::Database.new(db_file)
    @db.results_as_hash = true
    @db.type_translation = true

    init_db!

    @stmt_insert_fetches = @db.prepare %{
      INSERT INTO fetches (url, depth, domain_entry_id, step) VALUES (?,?,?,?)
    }
    @stmt_domain_entry_step = @db.prepare %{
      UPDATE domain_entry SET next_step = next_step + ? WHERE id = ?
    }
    @stmt_find_packet = @db.prepare %{
      SELECT url, depth, domain_entry_id FROM fetches
      WHERE processed_by IS NULL
      ORDER BY step
      LIMIT #{PACKET_SIZE}
    }
    @stmt_acquire_packet = @db.prepare %{ 
      UPDATE fetches SET processed_by = ?, acquired_at = ?
      WHERE url = ?
    }
    @stmt_finish_packet = @db.prepare %{
      UPDATE fetches SET processed_status = ?, released_at = ?
      WHERE url = ? AND processed_by = ?
    }
    @sql_next_step = "select next_step from domain_entry where id = ?".freeze
    @sql_count_url = "SELECT count(*) FROM fetches where url = ?".freeze
  end

  def add_urls(urls, depth, domain_entry_id)
    cnt = 0
    with_db do |db|
      next_step = db.get_first_value(@sql_next_step, domain_entry_id)

      urls.each do |url|
        if db.get_first_value(@sql_count_url, url) == 0
          @stmt_insert_fetches.execute!(url, depth, domain_entry_id, next_step + cnt)
          cnt += 1
        end
      end

      @stmt_domain_entry_step.execute!(cnt, domain_entry_id)
    end
    cnt
  end

  def add_work_packet()
    #'url1' => depth
  end

  def add_domain_entry(domain)
    with_db do |db|
      db.execute('INSERT OR IGNORE INTO domain_entry (domain) VALUES (?)', domain)
      db.get_first_value('SELECT id FROM domain_entry WHERE domain = ?', domain)
    end
  end

  def get_domain_entry(id)
    with_db do |db|
      db.get_first_row('SELECT * FROM domain_entry WHERE id = ?', id)
    end
  end

  #
  # pid:: Worker processor identificator, a String.
  #
  def acquire_work_packet(pid)
    urls = []

    with_db do
      @stmt_find_packet.execute!().each do |row|
        @stmt_acquire_packet.execute!(pid, Time.now, row['url'])
        urls << row #urls[row['url']] = row['depth']
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
    with_db do |db|
      packet.each do |url, status|
        @stmt_finish_packet.execute!(status, Time.now, url, pid)
      end
    end
  end

  protected

  #
  # Initialized the database schema if neccessary.
  #
  def init_db!
    begin
      version = @db.get_first_value('SELECT version FROM schema_info')
      return if version == SCHEMA_VERSION 
      raise "inconsistent database schema"
    rescue SQLite3::SQLException => e
      raise unless e.message =~ /no such table/
    end

    @db.execute %{
      CREATE TABLE domain_entry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain VARCHAR(100) UNIQUE NOT NULL,
        next_step INTEGER NOT NULL DEFAULT 0
      );
    }
    @db.execute %{
      CREATE TABLE fetches (
        url VARCHAR(2047) PRIMARY KEY,
        domain_entry_id INTEGER NOT NULL,
        depth INTEGER NOT NULL DEFAULT 0,
        processed_by VARCHAR(50), -- who is working on it right now
        processed_status VARCHAR(50),
        acquired_at TIMESTAMP,
        released_at TIMESTAMP,
        step INTEGER NOT NULL DEFAULT 0
      );
    }
    @db.execute %{
      CREATE UNIQUE INDEX fetches_url_idx ON fetches (url);
    }
    @db.execute %{
      CREATE INDEX fetches_processed_by_step_idx ON fetches (processed_by, step);
    }

    @db.execute %{
      CREATE TABLE schema_info (
        version INTEGER PRIMARY KEY
      );
    }

    @db.execute %{
      INSERT INTO schema_info (version) VALUES (#{SCHEMA_VERSION});
    }
  end

  def with_db
    res = nil
    @db_mutex.synchronize do
      @db.transaction do
        res = yield @db
      end
    end
    res
  end
end

if __FILE__ == $0
  m = Master.new('/tmp/master.db')

=begin
  1000.times do |domain|
    domain = "dom.#{domain}"
    urls = {}
    p domain
    1000.times do |i|
      urls["#{domain}/#{i}"] = 2
    end
    m.add_urls(urls, m.add_domain_entry(domain))
  end
  p "done"
=end
   
=begin
  #domains = %w(heise.de yahoo.com google.de meta.com ubuntu.com)
  domains.each do |domain|
    urls = {}
    1000.times do |i|
      urls[domain + "/" + (8000+i).to_s] = 2
    end
    urls[domain + "/1"] = 2
    if id = m.add_domain_entry(domain)
      p id
      m.add_urls(urls, id)
    end
  end
=end
  
  loop do
    packet = m.acquire_work_packet('abc')
    break if packet.empty?
    #x = packet.map do |r| r['url'] end
    #p x
    #puts "-----------------------------------"
  end

  #p m.acquire_work_packet('def')
  #p m.finish_work_packet('abc', 'hallo_leute' => 'OK')
end

