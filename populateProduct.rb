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

JOB_MYSQL_POOL_SIZE = 5
JOB_POOL_SIZE = 15
PRODUCT_MYSQL_POOL_SIZE = 30

class JobHandler

	def get_connection
		@db_job_pool.with do |db_connection|
			return db_connection
		end
	end

	def initialize(process_id,limit,site)	
		@jobs = Queue.new
		@db_job_pool = ConnectionPool.new(size: JOB_MYSQL_POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		
		statement = %{select * from raw_product_url where process_id = #{process_id} limit #{limit}}

		if (limit == -99)
			statement = %{select * from raw_product_url where process_id = #{process_id}}
		end

		db = self.get_connection
		@results = db.query(statement)
		@site = site

	end

	def run
		@results.size.times{|i| @jobs.push i}
		results_array = @results.each.to_a

		workers = (JOB_POOL_SIZE).times.map do
			Thread.new do
				begin      
			  		while x = @jobs.pop(true)
			  			url = %{https://www.walmart.com.br#{results_array[x]["url"]}}
			  			product = PoulateProductTable.new(url,@site)
			  			product.run
			  		end
				rescue ThreadError => ex
					puts "An error of type #{ex.class} happened, message is #{ex.message} [937]"
					puts %{#{url}}
				end
			end
		end

		workers.map(&:join)
	end
end

class PoulateProductTable

	def initialize(url,site)
		@@db_job_pool = ConnectionPool.new(size: PRODUCT_MYSQL_POOL_SIZE, timeout: 5) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		
		@url = url
		@db = self.get_connection
		@product = Hash.new
		@site = site
	end

	def get_connection
		@@db_job_pool.with do |db_connection|
			return db_connection
		end
	end

	def extract_model(page)
		local_parse = page.css("[@class='value-field Referencia-do-Modelo']").first

		if local_parse.to_s =~ />(.*)</ then
			@product["model"] = $1.downcase!		
		elsif page.to_s =~ /"value-field Referencia-do-Modelo">([A-Za-z0-9, -\/|]*)/ then
			@product["model"] = $1.downcase!		
		else
			## didnt find model above, try new tag
			local_parse = page.css("[@class='value-field Modelo']").first

			if local_parse.to_s =~ />(.*)</ then
				@product["model"] = $1.downcase!		
			elsif page.to_s =~ /"value-field Modelo">([A-Za-z0-9, -\/|]*)/ then
				@product["model"] = $1.downcase!
			end
		end
	end

	def parse_html_build_product
		begin
			local_page = Nokogiri::HTML(open(@url,:allow_redirections => :safe))
			local_parse = local_page.css("[@class='webstore product']")
			local_parse = local_parse.xpath("//script")
			local_parse.each do |script|
				if script.to_s.match('var dataLayer')
					if script.to_s  =~ /<script>var dataLayer = \[{"product":\s\[(.*)\],/ then 
						begin
							obj = JSON.parse($1, object_class: OpenStruct)
					 		@product["name"] = obj.productName
						 	@product["brandName"] = obj.productBrandName
						 	@product["departmentName"] = obj.productDepartmentName
						 	@product["categoryName"] = obj.productCategoryName
						 	@product["subcategoryName"] = obj.productSubCategory
						 	@product["productSku"] = obj.productSku
						 	@product["productSeller"] = Mysql.escape_string(obj.productSeller.to_s)
						 	@product["can_save"] = true
						
						rescue Exception => ex
							puts "An error of type #{ex.class} happened, message is #{ex.message} [736]"
						 	@product["can_save"] = false
						end
					end
				end
			end 

		rescue OpenURI::HTTPError => error
			puts "An error of type #{error.class} happened, message is #{error.message}] sleep (5) [774]"
			@product["can_save"] = false
	  		sleep(5)
		end

		self.extract_model(local_page)
	end

	def get_product_id
		statement = %{select id from product where model = "#{@product["model"]}";} 

		begin
			results = @db.query(statement)
			results.each do |row|
			  return row["id"]
			end	
		rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [4fr]"
			return -666
		end	
	end

	def insert_product
	   statement = "INSERT INTO product (name, brandName, departmentName, categoryName, subcategoryName,model,url,origin,targetSkuID,targetSourceID)
	   VALUES(\"#{@product['name']}\", \"#{@product['brandName']}\", \"#{@product['departmentName']}\",
	    \"#{@product['categoryName']}\", \"#{@product['subcategoryName']}\",\"#{@product['model']}\",
	    \"#{@url}\",\"#{@site}\",\"#{@product['productSku']}\",\"#{@product['productSeller']}\");"

	    begin
	    	@db.query(statement)
	    	@product["id"] = @db.last_id
	    rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [#{@product}] [638]"
	    end
	end

	def run
		#cep_array = ["15450","04510"]

		self.parse_html_build_product
		
		#can't save if threre is error or model is empty
		if (@product["can_save"] == false || @product["model"].nil?)
			return
		else
			self.insert_product
		end
		#self.fetchFreight(cep_array)
	end
end

#execution = JobHandler.new(20,-99,"walmart.com.br") # no limit on select
#execution = JobHandler.new(20,10,"walmart.com.br") # limit to 10 results
#execution = JobHandler.new(19,2000,"walmart.com.br") # limit to 10 results, development env
execution = JobHandler.new(19,-99,"walmart.com.br") # no limit on prod enviroment

execution.run #execute!
