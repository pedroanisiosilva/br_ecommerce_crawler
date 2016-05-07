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

JOB_MYSQL_POOL_SIZE = 1
JOB_POOL_SIZE = 10
PRODUCT_MYSQL_POOL_SIZE = 10

class JobHandler

	def get_connection
		@db_job_pool.with do |db_connection|
			return db_connection
		end
	end

	def initialize(limit,site)	
		@jobs = Queue.new
		@db_job_pool = ConnectionPool.new(size: JOB_MYSQL_POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		

		#statement = %{SELECT p.* FROM product p LEFT JOIN freight_data f ON f.product_id = p.id WHERE f.product_id IS NULL AND p.origin = "#{site}" LIMIT #{limit}}
		statement = %{SELECT p.* FROM product p LIMIT #{limit}}

		if (limit == -99)
			statement = %{select * from product}
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
		@@db_job_pool = ConnectionPool.new(size: PRODUCT_MYSQL_POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		
		@db = self.get_connection
		@product = product
		@site = site
	end

	def get_connection
		@@db_job_pool.with do |db_connection|
			return db_connection
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
		statement = "INSERT INTO product_related (product_id, url, target_site, updated_at)
	   VALUES(\"#{@product['id']}\", \"#{url}\", \"#{@site}\",\"#{Time.new.strftime(time_format)}\");"

    	begin
    		@db.query(statement)
    	rescue Exception => ex
    		puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
    	end			
	end

	def searchCompetitorWithGoogle
		url = %{https://www.google.com/search?q=site:#{@site} #{@product['name']} #{@product['model']}&num=100}
		search = parseUrl(url)
		links = search.scan(/<h3 class="r"><a href=[\'"]?([^\'" >]+)/).flatten
		results_array = links.each.to_a

		if results_array[0] =~ /\?url=([^&]+)/
			results_array[0] = $1
		elsif results_array[0] =~ /\?q=([^&]+)/
			results_array[0] = $1
		end
		results_array[0]
	end

	def run
		url = self.searchCompetitorWithBuscape

		if(url)
			self.insertIntoRelated(url)
		end
	end
end	

#execution = JobHandler.new(1000,"americanas.com.br") # limit to 10 results, development env
#execution = JobHandler.new(1000,"magazineluiza.com.br") # limit to 10 results, development env
#execution = JobHandler.new(1000,"casasbahia.com.br") # limit to 10 results, development env
execution = JobHandler.new(1000,"pontofrio.com.br") # limit to 10 results, development env


execution.run


#process["start_time"] = Time.new.strftime(time_format)
#http://www.buscape.com.br/cprocura/aspirador-de-po-mondial-next-1500-ap-12
#http://www.buscape.com.br/cprocura?produto=aspirador+de+p%F3+mondial+next+1500+ap+12&fromSearchBox=true





