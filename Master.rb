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
  def add_url(url, not_before=Time.now)
    @db_mutex.synchronize do
      begin
        @db.execute('INSERT INTO fetches (url, not_before) VALUES (?,?)', url, not_before)
        true
      rescue SQLite3::SQLException
        false
      end
    end
  end

  #
  # pid:: Worker processor identificator, a String.
  #
  def get_work_packet(pid, packet_size=50) 
    raise ArgumentError if packet_size > 500
    sql_find = "SELECT * FROM fetches WHERE processed_by IS NULL AND not_before < ? LIMIT #{packet_size}"
    sql_updt = "UPDATE fetches SET processed_by = ?, processed_on = ? WHERE url = ?"  
    urls = []

    @db_mutex.synchronize do
      @db.transaction do
        updt = @db.prepare(sql_updt)
        @db.execute(sql_find, Time.now).each {|row|
          updt.execute(pid, Time.now, row['url'])
          urls << row['url']
        }
      end
    end

    return urls
  end

  protected

  def init_db!
    @db.execute %{
      CREATE TABLE fetches (
        url VARCHAR(2048) PRIMARY KEY NOT NULL,
        not_before TIMESTAMP NOT NULL,
        processed_by VARCHAR(50), -- who is working on it right now
        processed_on TIMESTAMP,   -- when we started processing 
        processed_status VARCHAR(50)
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
  p m.add_url('hallo_leute') 
  p m.add_url('hallo_leute2') 
  p m.add_url('hallo_leute') 
  p m.add_url('blah') 
  p m.get_work_packet('abc')
end
