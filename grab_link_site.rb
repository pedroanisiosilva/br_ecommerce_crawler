#!/usr/bin/ruby

# Grab WALMART.COM product list

require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'mysql2'
require 'digest'
require 'socket'
require 'mysql'
require 'connection_pool'

POOL_SIZE = 15
$process_id_db = 0;
$db_connection_pool = ConnectionPool.new(size: POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}

jobs = Queue.new

def insert_process(process)
   statement = "INSERT INTO process (execution_registry, executed_file, executed_host, self_copy, pid,target_site,start_time)
   VALUES(\"#{process['execution_registry']}\", \"#{process['executed_file']}\", \"#{Mysql.escape_string(process['executed_host'])}\",
    \"#{Mysql.escape_string(process['self_copy'])}\", \"#{process['pid']}\",\"#{process['target_site']}\", \"#{process['start_time']}\");"

	$db_connection_pool.with do |db_connection|
	   db_connection.query(statement)
	   db_connection.last_id
	end
end

def insert_raw_product_url(process_id,url)
   statement = "INSERT INTO raw_product_url (process_id, url) VALUES(\"#{process_id}\", \"#{url}\");"
	$db_connection_pool.with do |db_connection|
	   db_connection.query(statement)
	end   
end

def register_process

	time_format = "%Y-%m-%d %H:%M:%S"

	process = Hash.new
	process["pid"] = Process.pid
	process["execution_registry"] = Digest::SHA256.base64digest %{#{Time.new.to_i}#{process["pid"]}}
	process["executed_file"] = __FILE__
	process["executed_host"] = Socket.gethostname
	process["self_copy"] = File.open(process["executed_file"], "rb").read
	process["target_site"] = "walmart.com.br"
	process["start_time"] = Time.new.strftime(time_format)

	insert_process(process)
end

def finisih_process
	process = Hash.new
	process["end_time"] = Time.new.strftime(time_format)
	time_format = "%Y-%m-%d %H:%M:%S"
	statement = "UPDATE process SET end_time = \"#{process['end_time']}\" WHERE id = #{$process_id_db};"

	$db_connection_pool.with do |db_connection|
	   db_connection.query(statement)
	end
end


def dump_data(data)
	file_path = "/Users/pasilv1/Dropbox/Synced/_personal/dev"
	file = %{#{file_path}/walmart_product_url.dump}
	File.open(file, 'a') {|f| f.write(%{#{data}\n})}
end

def load_page_number(number)
	url = %{https://www.walmart.com.br/busca/?ft=*&PS=40&PageNumber=#{number}} 
end

def parse_page_via_nokogiri(number)

	dump_to_file = false
	dump_to_mysql = true
	dump_to_screen = false

	begin
		local_page = Nokogiri::HTML(open(load_page_number(number),:allow_redirections => :safe))
		local_links = local_page.css("[@class='product-link']")

		local_links.each { |link| 

			if dump_to_screen
				puts link["href"]
			end

			if dump_to_file
				dump_data(link["href"])
			end

			if dump_to_mysql
				insert_raw_product_url($process_id_db,link["href"])
			end

		}

	rescue OpenURI::HTTPError => error
  		response = error.io
  		response.status
  		response.string

  		puts %{[#{response.status}][#{response.string}] sleep 5s}
  		sleep(5)
	end   
end

## begin execution

$process_id_db = register_process

20500.times{|i| jobs.push i}

workers = (POOL_SIZE).times.map do
  Thread.new do
    begin      
      while x = jobs.pop(true)
		parse_page_via_nokogiri(x)
      end
    rescue ThreadError
    end
  end
end

workers.map(&:join)

finisih_process





   










