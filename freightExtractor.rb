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
			open(URI.encode(url),"User-Agent" => agent) do |f|
				page_string = f.read
			end
		rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [hdh]"
		end			
		page_string
	end

	def db_insertFreightData(freight_name,freight_price,freight_promise,freight_raw_data,freight_destination)
		freight_revision = self.revisionNumber(freight_name,freight_destination,@product['id'],freight_price,freight_promise)

		if freight_revision >= 0
		    
		    begin
		    	statement = "INSERT INTO freight_data (product_id, freight_name, freight_cost, freight_promise, target_site,zip_code,freight_raw_data,freight_revision)
		   VALUES(\"#{@product['id']}\", \"#{freight_name}\", \"#{freight_price}\",\"#{freight_promise}\", 
		   \"#{@site}\",\"#{freight_destination}\",\"#{Mysql.escape_string(freight_raw_data.force_encoding(Encoding::UTF_8))}\",\"#{freight_revision}\");"		
		    	@db.query(statement)
		    rescue Exception => ex
		    	puts "An error of type #{ex.class} happened, message is #{ex.message} [7dh]"
		    end
		end	
	end	

	def revisionNumber(freight_name,zip_code,product_id,freight_cost,freight_promise)
		statement = %{SELECT f.freight_cost, f.freight_promise, f.freight_revision FROM freight_data f WHERE f.freight_name = '#{freight_name}' AND product_id = '#{product_id}' AND zip_code = #{zip_code} AND target_site='#{@site}';}
		results = @db.query(statement)

		if results.size == 0
			return 0
		end

		results.each do | result|
			puts %{result #{freight_promise}:#{result["freight_promise"]}}
			puts %{result #{freight_cost}:#{result["freight_cost"]}}
			if (freight_promise.to_i != result["freight_promise"].to_i || freight_cost.to_f != result["freight_cost"].to_f)
				puts %{increment revision}
				return result["freight_revision"]+1
			end
		end
		return -1
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

					self.db_insertFreightData(frete.name,frete.price,frete.shippingEstimateInDays,self.parseUrl(sniper),%{#{cep}000})

				end	
			rescue OpenURI::HTTPError => ex
				puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
				sleep(3)
			end 			    		
		end
	end	

	def run
		cep_SP = ['04538','06460','01001','08210','05859']
		cep_AC = ['69914']
		cep_AL = ['57030']
		cep_AM = ['69060']
		cep_AP = ['68908']
		cep_BA = ['41701']
		cep_CE = ['60337']
		cep_DF = ['70680']
		cep_ES = ['29055']
		cep_GO = ['74110']
		cep_MA = ['65077']
		cep_MG = ['30160']
		cep_MS = ['79023']
		cep_MT = ['78050']
		cep_PA = ['66635']
		cep_PB = ['58045']
		cep_PE = ['50710']
		cep_PI = ['64057']
		cep_PR = ['80730']
		cep_RJ = ['21240']
		cep_RN = ['59086']
		cep_RO = ['76805']
		cep_RR = ['69301']
		cep_RS = ['90250']
		cep_SC = ['88034']
		cep_SE = ['49160']
		cep_TO = ['77023']
		
		@cep_array = cep_SP + cep_AC + cep_SE + cep_SC + cep_RS + cep_RR + cep_RO
		@cep_array = @cep_array + cep_RN + cep_RJ + cep_PR + cep_PI + cep_PE + cep_PB + cep_PA + cep_MT
		@cep_array = @cep_array + cep_MS + cep_MG + cep_MA + cep_GO + cep_ES + cep_DF + cep_CE + cep_BA
		@cep_array = @cep_array + cep_AP + cep_AM + cep_AL + cep_TO

		self.fetchFreight(@cep_array)
	end
end

execution = JobHandler.new(1000,"walmart.com.br") # limit to 10 results
execution.run #execute!
