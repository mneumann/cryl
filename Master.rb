#
# The master maintains the central database.
#
class Master
  require 'sqlite3'
  require 'thread'
  require 'yaml'

  SCHEMA_VERSION = 1

  def initialize(db_file, packet_size)
    @db_mutex = Mutex.new
    @db = SQLite3::Database.new(db_file)
    @db.results_as_hash = true
    @db.type_translation = true

    init_db!

    @stmt_insert_fetches = @db.prepare %{
      INSERT INTO fetches (url, depth, domain_id, step) VALUES (?,?,?,?)
    }
    @stmt_updt_domains_next_step = @db.prepare %{
      UPDATE domains SET next_step = next_step + ? WHERE id = ?
    }
    @stmt_find_packet = @db.prepare %{
      SELECT url, depth, domain_id FROM fetches
      WHERE processed_by IS NULL
      ORDER BY step
      LIMIT #{packet_size}
    }
    @stmt_acquire_packet = @db.prepare %{ 
      UPDATE fetches SET processed_by = ?, acquired_at = ?
      WHERE url = ?
    }
    @stmt_finish_packet = @db.prepare %{
      UPDATE fetches SET processed_status = ?, released_at = ?
      WHERE url = ? AND processed_by = ?
    }
    @sql_next_step = "select next_step from domains where id = ?".freeze
    @sql_count_url = "SELECT count(*) FROM fetches where url = ?".freeze
  end

  def add_urls(urls, depth, domain_id)
    cnt = 0
    with_db do |db|
      next_step = db.get_first_value(@sql_next_step, domain_id)

      urls.each do |url|
        if db.get_first_value(@sql_count_url, url).to_i == 0
          @stmt_insert_fetches.execute!(url, depth, domain_id, next_step + cnt)
          cnt += 1
        end
      end

      @stmt_updt_domains_next_step.execute!(cnt, domain_id)
    end
    cnt
  end

  # Options:
  #
  #   :valid_content_types => [/text\//]
  #   :timeout => 3
  #   :same_domain_pattern => /pattern/
  #   :start_url
  #   :max_depth
  #
  def add(domain, prefs)
    id = add_domain(domain, prefs) || raise
    url = prefs[:start_url] || (domain + "/")
    add_urls([url], prefs[:max_depth] || 0, id)
  end

  def add_domain(domain, prefs)
    with_db do |db|
      begin
        db.execute('INSERT INTO domains (domain, prefs) VALUES (?, ?)', 
                   domain, YAML.dump(prefs))
        db.get_first_value('SELECT id FROM domains WHERE domain = ?', domain)
      rescue SQLite3::Exception
        nil
      end
    end
  end

  def get_domain_by_id(id)
    with_db do |db|
      db.get_first_row('SELECT * FROM domains WHERE id = ?', id)
    end
  end

  def get_domain_by_name(domain)
    with_db do |db|
      db.get_first_row('SELECT * FROM domains WHERE domain = ?', domain)
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
        urls << row 
      end
    end

    return urls
  end

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
      CREATE TABLE domains (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain VARCHAR(100) UNIQUE NOT NULL,
        prefs TEXT NOT NULL, 
        next_step INTEGER NOT NULL DEFAULT 0
      );
    }

    @db.execute %{
      CREATE TABLE fetches (
        url VARCHAR(2047) PRIMARY KEY,
        domain_id INTEGER NOT NULL,
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
