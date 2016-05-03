require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'mysql2'
require 'digest'
require 'socket'
require 'mysql'
require 'connection_pool'

POOL_SIZE = 15

class ProcessBase 

	def setup_execution
		case $site_number.to_i

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
				puts "You gave me #{$site_number} -- I have no idea what to do with that."
				exit()
		end		
	end

	def get_connection
		$db_connection_pool.with do |db_connection|
			return db_connection
		end
	end

	def insert_process(process)
	   statement = "INSERT INTO process (execution_registry, executed_file, executed_host, self_copy, pid,target_site,start_time)
	   VALUES(\"#{process['execution_registry']}\", \"#{process['executed_file']}\", \"#{Mysql.escape_string(process['executed_host'])}\",
	    \"#{Mysql.escape_string(process['self_copy'])}\", \"#{process['pid']}\",\"#{process['target_site']}\", \"#{process['start_time']}\");"

	    con = get_connection
	    con.query(statement)

	end

	def register_process(file)

		unless file
			file = __FILE__
		end

		time_format = "%Y-%m-%d %H:%M:%S"

		process = Hash.new
		process["pid"] = Process.pid
		process["execution_registry"] = Digest::SHA256.base64digest %{#{Time.new.to_i}#{process["pid"]}}
		process["executed_file"] = file
		process["executed_host"] = Socket.gethostname
		process["self_copy"] = File.open(process["executed_file"], "rb").read
		process["target_site"] = $execution['site']
		process["start_time"] = Time.new.strftime(time_format)

		insert_process(process)
	end	

	def initialize

		# sitenumber 1 - walmart.com.br
		# sitenumber 2 - pontofrio.com.br
		# sitebumber 3 - amazon.com

		$site_number = ARGV[0]
		unless $site_number
			puts "you need to provide the site number || assuming Walmart"
			$site_number = 1
		end

		$process_id_db = 0;
		$db_connection_pool = ConnectionPool.new(size: POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		
		$execution = Hash.new
		setup_execution()
		
	end

end



