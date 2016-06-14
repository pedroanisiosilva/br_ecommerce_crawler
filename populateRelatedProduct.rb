#!/usr/bin/ruby

require 'open-uri'
require 'openssl'
require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'json'
require 'ostruct'
require 'connection_pool'
require 'mysql2'
require 'mysql'
require 'useragents'
require 'net/http'
require 'uri'

JOB_MYSQL_POOL_SIZE = 5
JOB_POOL_SIZE = 25
PRODUCT_MYSQL_POOL_SIZE = 30

class JobHandler

	def get_connection
		@db_job_pool.with do |db_connection|
			return db_connection
		end
	end

	def initialize(limit,site)	
		@jobs = Queue.new
		@db_job_pool = ConnectionPool.new(size: JOB_MYSQL_POOL_SIZE, timeout: 1) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		
		statement = %{SELECT p.* FROM product p where p.url NOT IN (SELECT pr.url FROM product_related pr where pr.target_site="#{site}") LIMIT #{limit}}

		if (limit == -99)
			statement = %{SELECT p.* FROM product p where p.url NOT IN (SELECT pr.url FROM product_related pr where pr.target_site="#{site}")}
		end

		db = self.get_connection
		@results = db.query(statement)
		@site = site
	end

	def run
		@results.size.times{|i| @jobs.push i}
		results = @results.each

		workers = (JOB_POOL_SIZE).times.map do
			Thread.new do
				begin      
			  		while x = @jobs.pop(true)
			  			product = PopulateProductRelatedTable.new(results[x],@site)
			  			product.run
			  		end
				rescue ThreadError => ex
					puts "An error of type #{ex.class} happened, message is #{ex.message} [937]"
				end
			end
		end

		workers.map(&:join)
	end
end
 
class PopulateProductRelatedTable

	def initialize(product,site)
		@@db_job_pool = ConnectionPool.new(size: PRODUCT_MYSQL_POOL_SIZE, timeout: 1) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		
		@db = self.get_connection
		@product = product
		@site = site
	end

	def get_connection
		@@db_job_pool.with do |db_connection|
			begin
				db_connection.query("SELECT NOW()")
				return db_connection
			rescue
				sleep (1)
				self.get_connection
			end
		end
	end

	def parseUrl(url)
		uri = URI.escape(url)
		agent = UserAgents.rand()
		page_string = ""
		begin
			open(uri.to_s,{ssl_verify_mode: OpenSSL::SSL::VERIFY_NONE,"User-Agent" => agent} ) do |f|
				page_string = f.read
			end
		rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [hdh]"
		end			

		page_string
	end

	def searchCompetitorWithBuscape
		url=%{http://www.buscape.com.br/cprocura?produto=#{@product['name'].downcase!.gsub! " ", "-"}&fromSearchBox=true}
		search = parseUrl(url)
		links = search.scan(/redirect_url=([^ ;]+)/).flatten		
		results_array = links.each.to_a
		links2 = search.scan(/input value=\"(http:\/\/[^ ;]+)/).flatten
		results_array = results_array + links2.each.to_a
		clean_link = nil

		results_array.each do |link|
			
			if link.match(@site)
				clean_link = URI.decode(link)

				if clean_link =~ /([^ ?]+)/
					clean_link = $1
				end
			end
		end	
		return clean_link
	end

	def insertIntoRelated(url)
		time_format = "%Y-%m-%d %H:%M:%S"
		statement = "INSERT INTO product_related (product_id, url, target_site)
	   VALUES(\"#{@product['id']}\", \"#{url}\", \"#{@site}\");"

    	begin
    		@db.query(statement)
    	rescue Exception => ex
    		puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
    	end			
	end

	def run
		url = self.searchCompetitorWithBuscape

		if(url)
			self.insertIntoRelated(url)
		end
	end
end	

select_limit = 5000
site_name = ARGV[0]

execution = JobHandler.new(select_limit,site_name)
execution.run



