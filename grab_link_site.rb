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

# sitenumber 1 - walmart.com.br
# sitenumber 2 - pontofrio.com.br
# sitebumber 3 - amazon.com

site_number = ARGV[0]
unless site_number
	puts "you need to provide the site number"
end

POOL_SIZE = 15
$process_id_db = 0;
$db_connection_pool = ConnectionPool.new(size: POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}

def pagination_factor(factor)
  return Proc.new {|n| n*factor }
end

$execution = Hash.new

case site_number.to_i

	when 1

		$execution['site'] = "walmart.com.br"
		$execution['method'] = "search_pagination"
		$execution['search_url'] = "https://www.walmart.com.br/busca/?ft=*&PS=40&PageNumber="
		$execution['css_xpath'] = "[@class='product-link']"
		$execution['link_reference_xpath'] = "link['href']"
		$execution['pagination_factor'] = 1
		$execution['max_sku'] = 980000
		$execution['sku_per_page'] = 40


	when 2

		$execution['site'] = "pontofrio.com.br"
		$execution['method'] = "search_pagination"
		$execution['search_url'] = "http://search.pontofrio.com.br/search?p=Q&lbc=pontofrio-br&ts=custom&w=*&af=&isort=score&method=and&view=grid&srt="
		$execution['css_xpath'] = "[@class='link url']"
		$execution['link_reference_xpath'] = "link['href']"
		$execution['pagination_factor'] = 20
		$execution['max_sku'] = 140000000		
		$execution['sku_per_page'] = 20

	when 3

		$execution['site'] = "amazon.com"
		$execution['method'] = "search_pagination"
		$execution['search_url'] = "http://www.amazon.com/s/ref=sr_pg_2?rh=i%3Aaps%2Ck%3A%22.%22&keywords=%22.%22&page="
		$execution['css_xpath'] = "[@class='a-link-normal s-access-detail-page  a-text-normal']"
		$execution['link_reference_xpath'] = "link['href']"
		$execution['pagination_factor'] = 1
		$execution['max_sku'] = 536000000
		$execution['sku_per_page'] = 28		

	else
		puts "You gave me #{site_number} -- I have no idea what to do with that."
		exit()
	end		

$pagination = pagination_factor($execution['pagination_factor'])

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
	process["target_site"] = $execution['site']
	process["start_time"] = Time.new.strftime(time_format)

	insert_process(process)
end

def finisih_process
	process = Hash.new
	time_format = "%Y-%m-%d %H:%M:%S"	
	process["end_time"] = Time.new.strftime(time_format)
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
	url = %{#{$execution['search_url']}#{$pagination.call(number)}} 
end

def parse_page_via_nokogiri(number)

	dump_to_file = false
	dump_to_mysql = true
	dump_to_screen = false

	begin
		string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.56 Safari/536.5'

		local_page = Nokogiri::HTML(open(load_page_number(number),:allow_redirections => :safe, "User-Agent" => string))
		local_links = local_page.css($execution['css_xpath'])

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

($execution['max_sku']/$execution['sku_per_page']).to_i.times{|i| jobs.push i}

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





   










