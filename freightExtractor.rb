#/usr/bin/ruby

require './ProcessBase.rb'
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
JOB_POOL_SIZE = 5
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
		statement = %{SELECT p.* FROM product p LEFT JOIN freight_data f ON f.product_id = p.id WHERE f.product_id IS NULL LIMIT #{limit}}

		if (limit == -99)
			statement = %{select * from product where origin = "#{site}"}
		end

		db = self.get_connection
		@results = db.query(statement)
		@site = site
	end

	def run
		@results.size.times{|i| @jobs.push i}
		results = @results.each
		puts %{[#{@results.size}]}

		workers = (JOB_POOL_SIZE).times.map do
			Thread.new do
				begin      
			  		while x = @jobs.pop(true)
			  			product = PoulateFreightTable.new(results[x],@site)
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

class PoulateFreightTable

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
		agent = UserAgents.rand()
		page_string = ""
		begin
			open(url,"User-Agent" => agent) do |f|
				page_string = f.read
			end
		rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [hdh]"
		end			

		page_string
	end

	def fetchFreight(cep_input)
		sniper_url = ""
		cep_input.each do |cep|
			begin
				if (@site == "walmart.com.br")
					sniper = %{https://www2.walmart.com.br/checkout/services/simulation?postalCode=#{cep}000&sku=#{@product['targetSkuID']}&q=#{Random.rand(99999999999)}}
				end
				obj = JSON.parse(self.parseUrl(sniper), object_class: OpenStruct)

				obj[0].items[0].deliveryTypes.each do |frete|
					frete.price = frete.price.to_f/100
					statement = "INSERT INTO freight_data (product_id, freight_name, freight_cost, freight_promise, target_site,zip_code)
				   VALUES(\"#{@product['id']}\", \"#{frete.name}\", \"#{frete.price}\",
				    \"#{frete.shippingEstimateInDays}\", \"#{@site}\",\"#{cep}000\");"

			    	begin
			    		@db.query(statement)
			    	rescue Exception => ex
			    		puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
			    	end	
				end	
			rescue OpenURI::HTTPError => ex
				puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
				sleep(3)
			end 			    		
		end
	end	

	def run
		cep_sp = ['04538','06460','01001','08210','05859']
		cep_array = cep_sp
		self.fetchFreight(cep_array)
	end
end

#execution = JobHandler.new(20,-99,"walmart.com.br") # no limit on select
execution = JobHandler.new(10,"walmart.com.br") # limit to 10 results
#execution = JobHandler.new(5000,"walmart.com.br") # limit to 10 results, development env
#execution = JobHandler.new(10,"pontofrio.com.br") # limit to 10 results, development env

execution.run #execute!
