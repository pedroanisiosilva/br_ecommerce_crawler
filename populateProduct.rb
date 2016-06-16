#/usr/bin/ruby

#TODO: Add this script to run in crontab
#FIXME: Refactor excption handling
#TODO: Enable ERROR_LOG

require %{#{Dir.pwd}/ProcessBase.rb}
require 'rubygems'
require 'nokogiri'
require 'open_uri_redirections'
require 'json'
require 'ostruct'
require 'connection_pool'
require 'mysql2'
require 'mysql'
require 'json'
require 'bson'

JOB_MYSQL_POOL_SIZE = 1
JOB_POOL_SIZE = 20
PRODUCT_MYSQL_POOL_SIZE = 40

class JobHandler

	def get_connection
		@db_job_pool.with do |db_connection|
			return db_connection
		end
	end

	def initialize(process_id,limit,site)	
		@jobs = Queue.new
		@db_job_pool = ConnectionPool.new(size: JOB_MYSQL_POOL_SIZE, timeout: 1) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}		

		statement = %{SELECT r.* FROM raw_product_url r WHERE NOT exists (select null from product p WHERE r.url = p.url AND r.process_id = "#{process_id}") LIMIT #{limit}}

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
			  			#url = %{https://www.walmart.com.br#{results_array[x]["url"]}}
			  			 url = results_array[x]["url"]

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
		begin
			@@db_job_pool.nil?
		rescue
			@@db_job_pool = ConnectionPool.new(size: PRODUCT_MYSQL_POOL_SIZE, timeout: 1) { Mysql2::Client.new(:host => "localhost", :username => "root", :password => "hD@ba5MWUr#gnoyu95oX0*mF", :database => "COMMERCE_CRAWLER")}	
		end

		@url = url
		@db = self.get_connection
		@product = Hash.new
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

	def adjust_encode_and_escape(str)
		str
		#Mysql.escape_string(str)
		# begin
		# 	#str = str.encode!("ISO-8859-1", :undef => :replace, :invalid => :replace, :replace => "")
		# 	#str = Mysql.escape_string(str.encode!('UTF-8'))
		# rescue
		# 	#str = ""
		# end
	end

	def parse_html_build_product
		begin

			local_page = Nokogiri::HTML(open(%{https://www.walmart.com.br#{@url}},:allow_redirections => :safe))
			local_parse = local_page.css("[@class='webstore product']")
			local_parse = local_parse.xpath("//script")
			local_parse.each do |script|
				if script.to_s.match('var dataLayer')
					if script.to_s  =~ /<script>var dataLayer = \[{"product":\s\[(.*)\],/ then 
						begin
							site_txt = $1.gsub(/productDescription":\s"(.*)"/,'productDescription":"omitted"')
							obj = JSON.parse(site_txt, object_class: OpenStruct)

					 		@product["name"] = obj.productName
						 	@product["brandName"] = obj.productBrandName
						 	@product["departmentName"] = obj.productDepartmentName
						 	@product["categoryName"] = obj.productCategoryName
						 	@product["subcategoryName"] = obj.productSubCategory
						 	@product["productSku"] = obj.productSku
						 	@product["productSeller"] = self.adjust_encode_and_escape(obj.productSeller.to_s)
						 	@product["can_save"] = true
						 	@product["raw_data"] = self.adjust_encode_and_escape(site_txt)
						
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
		if !local_page.nil?
			self.extract_model(local_page)
		end
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
	    begin

			statement = @db.prepare("INSERT INTO product (name, brandName, departmentName, categoryName, subcategoryName,model,url,origin,targetSkuID,targetSourceID,raw_data) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
			statement.execute(@product['name'], @product['brandName'],@product['departmentName'],@product['categoryName'],@product['subcategoryName'],@product['model'],@url,@site,@product['productSku'],@product['productSeller'],@product['raw_data'])
			@product["id"] = @db.last_id	

	    rescue Exception => ex
			puts "An error of type #{ex.class} happened, message is #{ex.message} [#{@product}] [638]"
	    end
	end

	def check_if_exist
		statement = %{SELECT * FROM product p where p.url = "#{@url}"}
		begin
			results = @db.query(statement)

			if results.size == 0
				return false
			else
				return true
			end
		rescue
			return true
		end
	end		

	def run
		#cep_array = ["15450","04510"]

		if(self.check_if_exist)
			return
		else
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
end

#execution = JobHandler.new(20,-99,"walmart.com.br") # no limit on select
#execution = JobHandler.new(20,1,"walmart.com.br") # limit to 10 results
#execution = JobHandler.new(19,2000,"walmart.com.br") # limit to 10 results, development env
execution = JobHandler.new(172,10000,"walmart.com.br") # no limit on prod enviroment

execution.run #execute!
